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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InMemoryTrieTest {

  @Test
  public void testBasicPutAndGet() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    assertEquals(trie.size(), 0);

    assertNull(trie.put(toBytes("hello"), "world"));
    assertEquals(trie.size(), 1);
    assertEquals(trie.get(toBytes("hello")), "world");

    assertNull(trie.get(toBytes("hell")));
    assertNull(trie.get(toBytes("helloo")));
    assertNull(trie.get(toBytes("helloX")));
  }

  @Test
  public void testPutOverwrite() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    assertNull(trie.put(toBytes("key"), "value1"));
    assertEquals(trie.size(), 1);

    assertEquals(trie.put(toBytes("key"), "value2"), "value1");
    assertEquals(trie.size(), 1);
    assertEquals(trie.get(toBytes("key")), "value2");
  }

  @Test
  public void testMultipleKeys() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    trie.put(toBytes("apple"), 1);
    trie.put(toBytes("application"), 2);
    trie.put(toBytes("app"), 3);
    trie.put(toBytes("banana"), 4);
    trie.put(toBytes("band"), 5);

    assertEquals(trie.size(), 5);
    assertEquals(trie.get(toBytes("apple")), Integer.valueOf(1));
    assertEquals(trie.get(toBytes("application")), Integer.valueOf(2));
    assertEquals(trie.get(toBytes("app")), Integer.valueOf(3));
    assertEquals(trie.get(toBytes("banana")), Integer.valueOf(4));
    assertEquals(trie.get(toBytes("band")), Integer.valueOf(5));
  }

  @Test
  public void testPrefixKeys() {
    // Keys that are prefixes of each other
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    trie.put(toBytes("a"), 1);
    trie.put(toBytes("ab"), 2);
    trie.put(toBytes("abc"), 3);
    trie.put(toBytes("abcd"), 4);

    assertEquals(trie.size(), 4);
    assertEquals(trie.get(toBytes("a")), Integer.valueOf(1));
    assertEquals(trie.get(toBytes("ab")), Integer.valueOf(2));
    assertEquals(trie.get(toBytes("abc")), Integer.valueOf(3));
    assertEquals(trie.get(toBytes("abcd")), Integer.valueOf(4));

    // Insert in reverse order too
    InMemoryTrie<Integer> trie2 = new InMemoryTrie<>();
    trie2.put(toBytes("abcd"), 4);
    trie2.put(toBytes("abc"), 3);
    trie2.put(toBytes("ab"), 2);
    trie2.put(toBytes("a"), 1);

    assertEquals(trie2.size(), 4);
    assertEquals(trie2.get(toBytes("a")), Integer.valueOf(1));
    assertEquals(trie2.get(toBytes("ab")), Integer.valueOf(2));
    assertEquals(trie2.get(toBytes("abc")), Integer.valueOf(3));
    assertEquals(trie2.get(toBytes("abcd")), Integer.valueOf(4));
  }

  @Test
  public void testEmptyKey() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    assertNull(trie.put(new byte[0], "empty"));
    assertEquals(trie.size(), 1);
    assertEquals(trie.get(new byte[0]), "empty");

    // Can coexist with non-empty keys
    trie.put(toBytes("a"), "a");
    assertEquals(trie.size(), 2);
    assertEquals(trie.get(new byte[0]), "empty");
    assertEquals(trie.get(toBytes("a")), "a");
  }

  @Test
  public void testSingleByteKey() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    trie.put(new byte[]{0x00}, "zero");
    trie.put(new byte[]{0x01}, "one");
    trie.put(new byte[]{(byte) 0xFF}, "ff");

    assertEquals(trie.size(), 3);
    assertEquals(trie.get(new byte[]{0x00}), "zero");
    assertEquals(trie.get(new byte[]{0x01}), "one");
    assertEquals(trie.get(new byte[]{(byte) 0xFF}), "ff");
  }

  @Test
  public void testRemove() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    trie.put(toBytes("hello"), "world");
    trie.put(toBytes("help"), "me");

    assertEquals(trie.remove(toBytes("hello")), "world");
    assertEquals(trie.size(), 1);
    assertNull(trie.get(toBytes("hello")));
    assertEquals(trie.get(toBytes("help")), "me");

    // Remove non-existent key
    assertNull(trie.remove(toBytes("nonexistent")));
    assertEquals(trie.size(), 1);
  }

  @Test
  public void testConditionalRemove() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();
    String value = "world";
    trie.put(toBytes("hello"), value);

    // Wrong expected value — should not remove
    assertFalse(trie.remove(toBytes("hello"), "other"));
    assertEquals(trie.size(), 1);

    // Correct expected value — should remove (uses reference equality)
    assertTrue(trie.remove(toBytes("hello"), value));
    assertEquals(trie.size(), 0);
  }

  @Test
  public void testCompute() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();

    // Compute on non-existent key — insert
    Integer result = trie.compute(toBytes("counter"), (k, v) -> {
      assertNull(v);
      return 1;
    });
    assertEquals(result, Integer.valueOf(1));
    assertEquals(trie.size(), 1);

    // Compute on existing key — update
    result = trie.compute(toBytes("counter"), (k, v) -> {
      assertEquals(v, Integer.valueOf(1));
      return v + 1;
    });
    assertEquals(result, Integer.valueOf(2));
    assertEquals(trie.size(), 1);

    // Compute returning null on existing key — remove
    result = trie.compute(toBytes("counter"), (k, v) -> null);
    assertNull(result);
    assertEquals(trie.size(), 0);

    // Compute returning null on non-existent key — no-op
    result = trie.compute(toBytes("nonexistent"), (k, v) -> null);
    assertNull(result);
    assertEquals(trie.size(), 0);
  }

  @Test
  public void testComputeIfPresent() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    trie.put(toBytes("key"), 10);

    // Present — update
    Integer result = trie.computeIfPresent(toBytes("key"), (k, v) -> v * 2);
    assertEquals(result, Integer.valueOf(20));
    assertEquals(trie.size(), 1);

    // Present — remove via null return
    result = trie.computeIfPresent(toBytes("key"), (k, v) -> null);
    assertNull(result);
    assertEquals(trie.size(), 0);

    // Not present — no-op
    result = trie.computeIfPresent(toBytes("key"), (k, v) -> v * 2);
    assertNull(result);
    assertEquals(trie.size(), 0);
  }

  @Test
  public void testForEach() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    Map<String, Integer> expected = new HashMap<>();
    expected.put("apple", 1);
    expected.put("application", 2);
    expected.put("app", 3);
    expected.put("banana", 4);
    expected.put("band", 5);
    expected.put("", 0);

    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      trie.put(toBytes(entry.getKey()), entry.getValue());
    }

    Map<String, Integer> collected = new HashMap<>();
    trie.forEach((key, value) -> collected.put(new String(key, StandardCharsets.UTF_8), value));

    assertEquals(collected, expected);
  }

  @Test
  public void testNodeTypeTransitions() {
    // Force SparseNode -> DenseNode transition by inserting 7+ distinct first bytes
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    for (int i = 0; i < 10; i++) {
      byte[] key = new byte[]{(byte) i, 0x01};
      trie.put(key, i);
    }
    assertEquals(trie.size(), 10);

    // Verify all keys are retrievable
    for (int i = 0; i < 10; i++) {
      byte[] key = new byte[]{(byte) i, 0x01};
      assertEquals(trie.get(key), Integer.valueOf(i));
    }
  }

  @Test
  public void testChainSplitting() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();

    // Insert "abcdef" — creates a long chain
    trie.put(toBytes("abcdef"), "first");

    // Insert "abcxyz" — splits chain at 'c'/'d' divergence
    trie.put(toBytes("abcxyz"), "second");

    assertEquals(trie.size(), 2);
    assertEquals(trie.get(toBytes("abcdef")), "first");
    assertEquals(trie.get(toBytes("abcxyz")), "second");

    // Insert "ab" — splits at the prefix level
    trie.put(toBytes("ab"), "third");
    assertEquals(trie.size(), 3);
    assertEquals(trie.get(toBytes("ab")), "third");
    assertEquals(trie.get(toBytes("abcdef")), "first");
    assertEquals(trie.get(toBytes("abcxyz")), "second");
  }

  @Test
  public void testBinaryKeys() {
    // Test with hash-like binary keys (e.g., MD5 hashes)
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] key = new byte[16]; // 128-bit key
      for (int j = 0; j < 16; j++) {
        key[j] = (byte) ((i * 37 + j * 13) & 0xFF);
      }
      keys.add(key);
      trie.put(key, i);
    }

    assertEquals(trie.size(), 100);
    for (int i = 0; i < 100; i++) {
      assertEquals(trie.get(keys.get(i)), Integer.valueOf(i));
    }
  }

  @Test
  public void testSizeAccuracy() {
    InMemoryTrie<String> trie = new InMemoryTrie<>();

    // Inserts
    for (int i = 0; i < 50; i++) {
      trie.put(toBytes("key" + i), "val" + i);
    }
    assertEquals(trie.size(), 50);

    // Overwrites — size should not change
    for (int i = 0; i < 50; i++) {
      trie.put(toBytes("key" + i), "newval" + i);
    }
    assertEquals(trie.size(), 50);

    // Removals
    for (int i = 0; i < 25; i++) {
      trie.remove(toBytes("key" + i));
    }
    assertEquals(trie.size(), 25);

    // Compute insertions and removals
    trie.compute(toBytes("new1"), (k, v) -> "inserted");
    assertEquals(trie.size(), 26);
    trie.compute(toBytes("key25"), (k, v) -> null); // remove
    assertEquals(trie.size(), 25);
  }

  @Test
  public void testConcurrentReadWrite()
      throws Exception {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    int numKeys = 1000;
    int numThreads = 8;

    // Pre-populate
    for (int i = 0; i < numKeys; i++) {
      trie.put(toBytes("key" + i), i);
    }

    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    for (int t = 0; t < numThreads; t++) {
      int threadId = t;
      executor.submit(() -> {
        try {
          for (int i = 0; i < 1000; i++) {
            int keyIdx = (threadId * 1000 + i) % numKeys;
            String key = "key" + keyIdx;
            byte[] keyBytes = toBytes(key);

            // Mix of reads, writes, and computes
            switch (i % 4) {
              case 0:
                trie.get(keyBytes);
                break;
              case 1:
                trie.put(keyBytes, threadId * 1000 + i);
                break;
              case 2:
                trie.compute(keyBytes, (k, v) -> v == null ? 0 : v + 1);
                break;
              case 3:
                trie.computeIfPresent(keyBytes, (k, v) -> v + 1);
                break;
              default:
                break;
            }
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS), "Concurrent test timed out");
    executor.shutdown();
    assertEquals(errors.get(), 0, "Concurrent test had errors");
    // Size should be exactly numKeys (no keys added or removed)
    assertEquals(trie.size(), numKeys);
  }

  @Test
  public void testForEachCompleteness() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    Map<String, Integer> inserted = new HashMap<>();

    // Insert various key patterns
    String[] keys = {"", "a", "ab", "abc", "b", "ba", "bac", "z", "za", "zzzz"};
    for (int i = 0; i < keys.length; i++) {
      trie.put(toBytes(keys[i]), i);
      inserted.put(keys[i], i);
    }

    Map<String, Integer> collected = new HashMap<>();
    trie.forEach((key, value) -> collected.put(new String(key, StandardCharsets.UTF_8), value));

    assertEquals(collected.size(), inserted.size());
    assertEquals(collected, inserted);
  }

  @Test
  public void testLargeNumberOfKeys() {
    InMemoryTrie<Integer> trie = new InMemoryTrie<>();
    int numKeys = 10000;

    for (int i = 0; i < numKeys; i++) {
      trie.put(toBytes("key" + String.format("%06d", i)), i);
    }
    assertEquals(trie.size(), numKeys);

    for (int i = 0; i < numKeys; i++) {
      assertEquals(trie.get(toBytes("key" + String.format("%06d", i))), Integer.valueOf(i));
    }

    // Remove half
    for (int i = 0; i < numKeys / 2; i++) {
      trie.remove(toBytes("key" + String.format("%06d", i)));
    }
    assertEquals(trie.size(), numKeys / 2);
  }

  private static byte[] toBytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
