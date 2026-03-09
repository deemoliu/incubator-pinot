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
 * Abstract base class for trie nodes. Each node may hold a value (if it is a terminal node)
 * and has children keyed by individual bytes.
 *
 * @param <V> the type of value stored at terminal nodes
 */
abstract class TrieNode<V> {
  volatile V _value;

  @Nullable
  abstract TrieNode<V> getChild(byte b);

  abstract TrieNode<V> setChild(byte b, TrieNode<V> child);

  abstract void forEachChild(BiConsumer<Byte, TrieNode<V>> action);

  abstract int childCount();
}
