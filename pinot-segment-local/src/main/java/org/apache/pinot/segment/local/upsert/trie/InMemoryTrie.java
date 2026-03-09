/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert.trie;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import javax.annotation.Nullable;


/**
 * Thread-safe in-memory trie (prefix tree) data structure that maps byte[] keys to values.
 * Uses path compression ({@link ChainNode}), sparse branching ({@link SparseNode}),
 * and dense branching ({@link DenseNode}) for memory efficiency.
 *
 * <p>Thread safety is achieved via a {@link ReentrantReadWriteLock}: reads can proceed concurrently,
 * while writes are exclusive.
 *
 * <p>The trie uses a simple design where branching nodes (SparseNode, DenseNode) own children keyed
 * by single bytes, and ChainNode provides path compression for sequences of single-child nodes.
 * The root is always a branching node (never a ChainNode). Values are stored directly at the
 * branching node that terminates a key.
 *
 * @param <V> the type of value stored in the trie
 */
public class InMemoryTrie<V> {
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  // Root is volatile to support SparseNode -> DenseNode promotion
  @SuppressWarnings("unchecked")
  private volatile TrieNode<V> _root = new SparseNode<>(new byte[0], new TrieNode[0]);
  private final AtomicLong _size = new AtomicLong(0);

  /**
   * Returns the number of key-value pairs in the trie.
   */
  public long size() {
    return _size.get();
  }

  /**
   * Retrieves the value associated with the given key.
   */
  @Nullable
  public V get(byte[] key) {
    _lock.readLock().lock();
    try {
      TrieNode<V> node = findNode(key, 0, _root);
      return node != null ? node._value : null;
    } finally {
      _lock.readLock().unlock();
    }
  }

  /**
   * Inserts a key-value pair, returning the previous value (or null).
   */
  @Nullable
  public V put(byte[] key, V value) {
    _lock.writeLock().lock();
    try {
      V[] prev = newValueHolder();
      _root = putInternal(_root, key, 0, value, prev);
      if (prev[0] == null) {
        _size.incrementAndGet();
      }
      return prev[0];
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /**
   * Atomically computes a new value for the given key. Matches {@code ConcurrentHashMap.compute()} semantics:
   * if the function returns null and the key existed, the entry is removed; if it returns non-null, the value
   * is inserted/updated.
   */
  @Nullable
  public V compute(byte[] key, BiFunction<byte[], V, V> remappingFunction) {
    _lock.writeLock().lock();
    try {
      TrieNode<V> node = findNode(key, 0, _root);
      V oldValue = node != null ? node._value : null;
      V newValue = remappingFunction.apply(key, oldValue);
      if (newValue != null) {
        if (oldValue != null) {
          // Update existing node value in place
          node._value = newValue;
        } else {
          // Insert new key
          V[] prev = newValueHolder();
          _root = putInternal(_root, key, 0, newValue, prev);
          _size.incrementAndGet();
        }
      } else if (oldValue != null) {
        // Remove existing key (lazy removal — just null out value)
        node._value = null;
        _size.decrementAndGet();
      }
      return newValue;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /**
   * Atomically computes a new value only if the key is already present.
   */
  @Nullable
  public V computeIfPresent(byte[] key, BiFunction<byte[], V, V> remappingFunction) {
    _lock.writeLock().lock();
    try {
      TrieNode<V> node = findNode(key, 0, _root);
      if (node == null || node._value == null) {
        return null;
      }
      V newValue = remappingFunction.apply(key, node._value);
      if (newValue != null) {
        node._value = newValue;
      } else {
        node._value = null;
        _size.decrementAndGet();
      }
      return newValue;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /**
   * Removes the entry for the given key only if it is currently mapped to the expected value.
   */
  public boolean remove(byte[] key, V expectedValue) {
    _lock.writeLock().lock();
    try {
      TrieNode<V> node = findNode(key, 0, _root);
      if (node != null && node._value == expectedValue) {
        node._value = null;
        _size.decrementAndGet();
        return true;
      }
      return false;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /**
   * Removes the entry for the given key, returning the previous value.
   */
  @Nullable
  public V remove(byte[] key) {
    _lock.writeLock().lock();
    try {
      TrieNode<V> node = findNode(key, 0, _root);
      if (node != null && node._value != null) {
        V old = node._value;
        node._value = null;
        _size.decrementAndGet();
        return old;
      }
      return null;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /**
   * Iterates over all key-value pairs in the trie via DFS.
   */
  public void forEach(BiConsumer<byte[], V> action) {
    _lock.readLock().lock();
    try {
      forEachDfs(_root, new byte[0], action);
    } finally {
      _lock.readLock().unlock();
    }
  }

  // ---- Internal methods ----

  /**
   * Finds the branching node that corresponds to the given key.
   * Returns the node if the key path exists (node may have null value for non-terminal).
   * Returns null if the key path does not exist in the trie.
   *
   * <p>Navigation logic:
   * - At a branching node (Sparse/Dense), look up child by key[pos]. If child is a ChainNode,
   *   match the chain bytes against key[pos+1..]. If child is a branching node, recurse into it.
   * - When pos == key.length, the current branching node is the terminal for this key.
   */
  @Nullable
  private TrieNode<V> findNode(byte[] key, int pos, TrieNode<V> node) {
    if (pos == key.length) {
      return node;
    }

    TrieNode<V> child = node.getChild(key[pos]);
    if (child == null) {
      return null;
    }

    if (child instanceof ChainNode) {
      ChainNode<V> chain = (ChainNode<V>) child;
      byte[] chainBytes = chain._chainBytes;
      int matchLen = matchLength(key, pos + 1, chainBytes, 0, chainBytes.length);
      if (matchLen < chainBytes.length) {
        return null; // key diverges mid-chain
      }
      return findNode(key, pos + 1 + chainBytes.length, chain._child);
    }

    return findNode(key, pos + 1, child);
  }

  /**
   * Inserts a key-value pair into the trie rooted at {@code node}, returning the (possibly new)
   * root node. This handles node creation, chain splitting, and sparse-to-dense promotion.
   *
   * @param node current branching node
   * @param key the full key bytes
   * @param pos current position in key
   * @param value value to insert
   * @param prevHolder single-element array to receive the previous value (if any)
   * @return the (possibly replaced) node
   */
  @SuppressWarnings("unchecked")
  private TrieNode<V> putInternal(TrieNode<V> node, byte[] key, int pos, V value, V[] prevHolder) {
    if (pos == key.length) {
      prevHolder[0] = node._value;
      node._value = value;
      return node;
    }

    byte b = key[pos];
    TrieNode<V> child = node.getChild(b);

    if (child == null) {
      // Create leaf for remaining key bytes
      TrieNode<V> leaf = new SparseNode<>(new byte[0], new TrieNode[0]);
      leaf._value = value;
      TrieNode<V> newChild = wrapInChain(key, pos + 1, leaf);
      return node.setChild(b, newChild);
    }

    if (child instanceof ChainNode) {
      ChainNode<V> chain = (ChainNode<V>) child;
      byte[] chainBytes = chain._chainBytes;
      int matchLen = matchLength(key, pos + 1, chainBytes, 0, chainBytes.length);

      if (matchLen == chainBytes.length) {
        // Full chain match — recurse into chain's child
        TrieNode<V> newChainChild = putInternal(chain._child, key, pos + 1 + chainBytes.length, value, prevHolder);
        if (newChainChild != chain._child) {
          chain._child = newChainChild;
        }
        return node;
      }

      // Partial match — split the chain
      TrieNode<V> newChild = splitAndInsert(chain, chainBytes, key, pos + 1, matchLen, value);
      return node.setChild(b, newChild);
    }

    // Branching node child — recurse
    TrieNode<V> newChild = putInternal(child, key, pos + 1, value, prevHolder);
    if (newChild != child) {
      return node.setChild(b, newChild);
    }
    return node;
  }

  /**
   * Splits a chain at the divergence point and inserts the new key.
   * Returns the new node to replace the chain.
   */
  @SuppressWarnings("unchecked")
  private TrieNode<V> splitAndInsert(ChainNode<V> chain, byte[] chainBytes,
      byte[] key, int keyPos, int matchLen, V value) {
    // Build existing child for the portion of chain after the split
    byte existingByte = chainBytes[matchLen];
    TrieNode<V> existingChild = wrapInChain(chainBytes, matchLen + 1, chain._child);

    // Build new branch node at the divergence point
    TrieNode<V> branchNode;
    int newKeyPos = keyPos + matchLen;

    if (newKeyPos >= key.length) {
      // New key ends at the branch point (prefix of existing chain)
      branchNode = new SparseNode<>(new byte[]{existingByte}, new TrieNode[]{existingChild});
      branchNode._value = value;
    } else {
      byte newByte = key[newKeyPos];
      TrieNode<V> newLeaf = new SparseNode<>(new byte[0], new TrieNode[0]);
      newLeaf._value = value;
      TrieNode<V> newChild = wrapInChain(key, newKeyPos + 1, newLeaf);

      if (Byte.compareUnsigned(existingByte, newByte) < 0) {
        branchNode = new SparseNode<>(new byte[]{existingByte, newByte},
            new TrieNode[]{existingChild, newChild});
      } else {
        branchNode = new SparseNode<>(new byte[]{newByte, existingByte},
            new TrieNode[]{newChild, existingChild});
      }
    }

    // Wrap branch node with common prefix chain if needed
    return wrapInChain(chainBytes, 0, matchLen, branchNode);
  }

  /**
   * Creates a ChainNode for bytes[from..to) leading to target, or returns target if range is empty.
   */
  private TrieNode<V> wrapInChain(byte[] bytes, int from, int to, TrieNode<V> target) {
    if (from >= to) {
      return target;
    }
    return new ChainNode<>(Arrays.copyOfRange(bytes, from, to), target);
  }

  /**
   * Creates a ChainNode for key[from..key.length) leading to target, or returns target if range is empty.
   */
  private TrieNode<V> wrapInChain(byte[] key, int from, TrieNode<V> target) {
    return wrapInChain(key, from, key.length, target);
  }

  /**
   * DFS traversal that reconstructs keys and calls action for terminal nodes.
   */
  private void forEachDfs(TrieNode<V> node, byte[] prefix, BiConsumer<byte[], V> action) {
    if (node._value != null) {
      action.accept(prefix, node._value);
    }

    node.forEachChild((b, child) -> {
      byte[] childPrefix = appendByte(prefix, b);
      if (child instanceof ChainNode) {
        ChainNode<V> chain = (ChainNode<V>) child;
        byte[] chainPrefix = concat(childPrefix, chain._chainBytes);
        forEachDfs(chain._child, chainPrefix, action);
      } else {
        forEachDfs(child, childPrefix, action);
      }
    });
  }

  /**
   * Returns the length of the common prefix between a[aOff..] and b[bOff..bEnd).
   */
  static int matchLength(byte[] a, int aOff, byte[] b, int bOff, int bEnd) {
    int i = 0;
    int maxLen = Math.min(a.length - aOff, bEnd - bOff);
    while (i < maxLen && a[aOff + i] == b[bOff + i]) {
      i++;
    }
    return i;
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }

  private static byte[] appendByte(byte[] arr, byte b) {
    byte[] result = new byte[arr.length + 1];
    System.arraycopy(arr, 0, result, 0, arr.length);
    result[arr.length] = b;
    return result;
  }

  @SuppressWarnings("unchecked")
  private V[] newValueHolder() {
    return (V[]) new Object[1];
  }
}
