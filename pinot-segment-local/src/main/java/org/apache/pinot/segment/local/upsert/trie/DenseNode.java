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
 * Dense trie node for 7+ children. Uses a 256-element array indexed by unsigned byte value.
 *
 * @param <V> the type of value stored at terminal nodes
 */
class DenseNode<V> extends TrieNode<V> {
  final TrieNode<V>[] _children;

  @SuppressWarnings("unchecked")
  DenseNode(TrieNode<V>[] children) {
    _children = children;
  }

  @Override
  @Nullable
  TrieNode<V> getChild(byte b) {
    return _children[Byte.toUnsignedInt(b)];
  }

  @Override
  TrieNode<V> setChild(byte b, TrieNode<V> child) {
    _children[Byte.toUnsignedInt(b)] = child;
    return this;
  }

  @Override
  void forEachChild(BiConsumer<Byte, TrieNode<V>> action) {
    for (int i = 0; i < 256; i++) {
      if (_children[i] != null) {
        action.accept((byte) i, _children[i]);
      }
    }
  }

  @Override
  int childCount() {
    int count = 0;
    for (TrieNode<V> child : _children) {
      if (child != null) {
        count++;
      }
    }
    return count;
  }
}
