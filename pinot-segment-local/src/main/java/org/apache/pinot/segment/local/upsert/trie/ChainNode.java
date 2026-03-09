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
 * Path-compressed trie node that stores a chain of bytes leading to a single child.
 * ChainNodes are edge labels between branching nodes (SparseNode/DenseNode).
 * They do not participate in the normal getChild/setChild branching — instead,
 * the trie traversal logic handles them specially by matching chain bytes.
 *
 * @param <V> the type of value stored at terminal nodes
 */
class ChainNode<V> extends TrieNode<V> {
  byte[] _chainBytes;
  volatile TrieNode<V> _child;

  ChainNode(byte[] chainBytes, TrieNode<V> child) {
    _chainBytes = chainBytes;
    _child = child;
  }

  @Override
  @Nullable
  TrieNode<V> getChild(byte b) {
    // ChainNodes are not used as branching nodes in the trie structure.
    // They are handled as edge labels by InMemoryTrie's find/put logic.
    throw new UnsupportedOperationException("ChainNode does not support getChild");
  }

  @Override
  TrieNode<V> setChild(byte b, TrieNode<V> child) {
    throw new UnsupportedOperationException("ChainNode does not support setChild");
  }

  @Override
  void forEachChild(BiConsumer<Byte, TrieNode<V>> action) {
    // ChainNodes are traversed by InMemoryTrie.forEachDfs, not via this method
    throw new UnsupportedOperationException("ChainNode does not support forEachChild");
  }

  @Override
  int childCount() {
    return 1;
  }
}
