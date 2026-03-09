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

import java.util.function.BiConsumer;
import javax.annotation.Nullable;


/**
 * Sparse trie node for 2-6 children. Uses sorted byte keys and a parallel children array
 * for compact representation.
 *
 * @param <V> the type of value stored at terminal nodes
 */
class SparseNode<V> extends TrieNode<V> {
  static final int MAX_SPARSE_CHILDREN = 6;

  byte[] _keys;
  TrieNode<V>[] _children;

  @SuppressWarnings("unchecked")
  SparseNode(byte[] keys, TrieNode<V>[] children) {
    _keys = keys;
    _children = children;
  }

  @Override
  @Nullable
  TrieNode<V> getChild(byte b) {
    int idx = indexOf(b);
    return idx >= 0 ? _children[idx] : null;
  }

  @Override
  @SuppressWarnings("unchecked")
  TrieNode<V> setChild(byte b, TrieNode<V> child) {
    int idx = indexOf(b);
    if (idx >= 0) {
      _children[idx] = child;
      return this;
    }
    // Insert in sorted order
    int insertionPoint = -(idx + 1);
    int newLen = _keys.length + 1;
    if (newLen > MAX_SPARSE_CHILDREN) {
      // Promote to DenseNode
      return promoteToDense(b, child);
    }
    byte[] newKeys = new byte[newLen];
    TrieNode<V>[] newChildren = new TrieNode[newLen];
    System.arraycopy(_keys, 0, newKeys, 0, insertionPoint);
    System.arraycopy(_children, 0, newChildren, 0, insertionPoint);
    newKeys[insertionPoint] = b;
    newChildren[insertionPoint] = child;
    System.arraycopy(_keys, insertionPoint, newKeys, insertionPoint + 1, _keys.length - insertionPoint);
    System.arraycopy(_children, insertionPoint, newChildren, insertionPoint + 1, _children.length - insertionPoint);
    _keys = newKeys;
    _children = newChildren;
    return this;
  }

  @Override
  void forEachChild(BiConsumer<Byte, TrieNode<V>> action) {
    for (int i = 0; i < _keys.length; i++) {
      action.accept(_keys[i], _children[i]);
    }
  }

  @Override
  int childCount() {
    return _keys.length;
  }

  private int indexOf(byte b) {
    // Binary search treating bytes as unsigned for correct ordering
    int lo = 0;
    int hi = _keys.length - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int cmp = Byte.compareUnsigned(_keys[mid], b);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid - 1;
      } else {
        return mid;
      }
    }
    return -(lo + 1);
  }

  @SuppressWarnings("unchecked")
  private DenseNode<V> promoteToDense(byte b, TrieNode<V> child) {
    TrieNode<V>[] denseChildren = new TrieNode[256];
    for (int i = 0; i < _keys.length; i++) {
      denseChildren[Byte.toUnsignedInt(_keys[i])] = _children[i];
    }
    denseChildren[Byte.toUnsignedInt(b)] = child;
    DenseNode<V> denseNode = new DenseNode<>(denseChildren);
    denseNode._value = _value;
    return denseNode;
  }
}
