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
package org.apache.pinot.segment.local.upsert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.upsert.TriePartitionUpsertMetadataManager.RecordLocation;
import org.apache.pinot.segment.local.upsert.trie.InMemoryTrie;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TriePartitionUpsertMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final List<String> PRIMARY_KEY_COLUMNS = Collections.singletonList("pk");
  private static final List<String> COMPARISON_COLUMNS = Collections.singletonList("timeCol");
  private static final String DELETE_RECORD_COLUMN = "deleteCol";
  private static final int MOCK_FALLBACK_BASE_OFFSET = 1000;

  private UpsertContext.Builder _contextBuilder;

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUpContextBuilder() {
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(
        new java.io.File(System.getProperty("java.io.tmpdir"), "TriePartitionUpsertTest"));
    _contextBuilder = new UpsertContext.Builder()
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(PRIMARY_KEY_COLUMNS)
        .setComparisonColumns(COMPARISON_COLUMNS);
  }

  @AfterClass
  public void tearDown() {
    // No-op, no persistent files created
  }

  @Test
  public void testStartFinishOperation() {
    TriePartitionUpsertMetadataManager upsertMetadataManager =
        new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, _contextBuilder.build());

    assertTrue(upsertMetadataManager.startOperation());
    assertTrue(upsertMetadataManager.startOperation());

    AtomicBoolean stopped = new AtomicBoolean();
    AtomicBoolean closed = new AtomicBoolean();
    ExecutorService executor = Executors.newFixedThreadPool(1);
    executor.submit(() -> {
      upsertMetadataManager.stop();
      stopped.set(true);
      try {
        upsertMetadataManager.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      closed.set(true);
    });
    executor.shutdown();

    TestUtils.waitForCondition(aVoid -> stopped.get(), 10_000L, "Failed to stop the metadata manager");
    assertFalse(closed.get());
    assertFalse(upsertMetadataManager.startOperation());

    upsertMetadataManager.finishOperation();
    assertFalse(closed.get());
    upsertMetadataManager.finishOperation();
    TestUtils.waitForCondition(aVoid -> closed.get(), 10_000L, "Failed to close the metadata manager");
  }

  @Test
  public void testAddReplaceRemoveSegment()
      throws IOException {
    verifyAddReplaceRemoveSegment(HashFunction.NONE, false);
    verifyAddReplaceRemoveSegment(HashFunction.MD5, false);
    verifyAddReplaceRemoveSegment(HashFunction.MURMUR3, false);
    verifyAddReplaceRemoveSegment(HashFunction.NONE, true);
    verifyAddReplaceRemoveSegment(HashFunction.MD5, true);
    verifyAddReplaceRemoveSegment(HashFunction.MURMUR3, true);
  }

  private void verifyAddReplaceRemoveSegment(HashFunction hashFunction, boolean enableSnapshot)
      throws IOException {
    TriePartitionUpsertMetadataManager upsertMetadataManager =
        new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0,
            _contextBuilder.setHashFunction(hashFunction).build());
    InMemoryTrie<RecordLocation> trie = upsertMetadataManager._primaryKeyTrie;
    Set<IndexSegment> trackedSegments = upsertMetadataManager._trackedSegments;

    // Add the first segment
    int numRecords = 6;
    int[] primaryKeys = new int[]{0, 1, 2, 0, 1, 0};
    int[] timestamps = new int[]{100, 100, 100, 80, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    List<PrimaryKey> primaryKeys1 = getPrimaryKeyList(numRecords, primaryKeys);
    ImmutableSegmentImpl segment1 = mockImmutableSegment(1, validDocIds1, null, primaryKeys1);
    List<RecordInfo> recordInfoList1;
    if (enableSnapshot) {
      int[] docIds1 = new int[]{2, 4, 5};
      MutableRoaringBitmap validDocIdsSnapshot1 = new MutableRoaringBitmap();
      validDocIdsSnapshot1.add(docIds1);
      recordInfoList1 = getRecordInfoList(validDocIdsSnapshot1, primaryKeys, timestamps, null);
    } else {
      recordInfoList1 = getRecordInfoList(numRecords, primaryKeys, timestamps, null);
    }
    upsertMetadataManager.addSegment(segment1, validDocIds1, null, recordInfoList1.iterator());
    trackedSegments.add(segment1);

    // segment1: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
    assertEquals(trie.size(), 3);
    checkRecordLocation(trie, 0, segment1, 5, 100, hashFunction);
    checkRecordLocation(trie, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(trie, 2, segment1, 2, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{2, 4, 5});

    // Add the second segment
    numRecords = 5;
    primaryKeys = new int[]{0, 1, 2, 3, 0};
    timestamps = new int[]{100, 100, 120, 80, 80};
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment2 =
        mockImmutableSegment(2, validDocIds2, null, getPrimaryKeyList(numRecords, primaryKeys));
    List<RecordInfo> recordInfoList2;
    if (enableSnapshot) {
      MutableRoaringBitmap validDocIdsSnapshot2 = new MutableRoaringBitmap();
      validDocIdsSnapshot2.add(0, 2, 3);
      recordInfoList2 = getRecordInfoList(validDocIdsSnapshot2, primaryKeys, timestamps, null);
    } else {
      recordInfoList2 = getRecordInfoList(numRecords, primaryKeys, timestamps, null);
    }
    upsertMetadataManager.addSegment(segment2, validDocIds2, null, recordInfoList2.iterator());
    trackedSegments.add(segment2);

    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(trie.size(), 4);
    checkRecordLocation(trie, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(trie, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(trie, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(trie, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});

    // Add an empty segment
    EmptyIndexSegment emptySegment = mockEmptySegment(3);
    upsertMetadataManager.addSegment(emptySegment);
    assertEquals(trie.size(), 4);

    // Replace (reload) the first segment
    ThreadSafeMutableRoaringBitmap newValidDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl newSegment1 = mockImmutableSegment(1, newValidDocIds1, null, primaryKeys1);
    upsertMetadataManager.replaceSegment(newSegment1, newValidDocIds1, null, recordInfoList1.iterator(), segment1);
    trackedSegments.add(newSegment1);
    trackedSegments.remove(segment1);

    assertEquals(trie.size(), 4);
    checkRecordLocation(trie, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(trie, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(trie, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(trie, 3, segment2, 3, 80, hashFunction);
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});

    // Remove segment2
    upsertMetadataManager.removeSegment(segment2);
    assertEquals(trie.size(), 1);
    checkRecordLocation(trie, 1, newSegment1, 4, 120, hashFunction);
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(trackedSegments, Collections.singleton(newSegment1));

    upsertMetadataManager.stop();
    upsertMetadataManager.close();
  }

  @Test
  public void testAddRecord()
      throws IOException {
    verifyAddRecord(HashFunction.NONE);
    verifyAddRecord(HashFunction.MD5);
    verifyAddRecord(HashFunction.MURMUR3);
  }

  private void verifyAddRecord(HashFunction hashFunction)
      throws IOException {
    TriePartitionUpsertMetadataManager upsertMetadataManager =
        new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0,
            _contextBuilder.setHashFunction(hashFunction).build());
    InMemoryTrie<RecordLocation> trie = upsertMetadataManager._primaryKeyTrie;

    // addRecord
    MutableSegment segment = mockMutableSegment(1, new ThreadSafeMutableRoaringBitmap(), null);
    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 0, new IntWrapper(100), false));
    // 0 -> {0, 100}
    checkRecordLocation(trie, 0, segment, 0, 100, hashFunction);

    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(1), 1, new IntWrapper(120), false));
    // 0 -> {0, 100}, 1 -> {1, 120}
    checkRecordLocation(trie, 1, segment, 1, 120, hashFunction);

    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 2, new IntWrapper(80), false));
    // 0 -> {0, 100} (not updated — out of order), 1 -> {1, 120}
    checkRecordLocation(trie, 0, segment, 0, 100, hashFunction);

    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 3, new IntWrapper(120), false));
    // 0 -> {3, 120} (updated — same or newer), 1 -> {1, 120}
    checkRecordLocation(trie, 0, segment, 3, 120, hashFunction);

    assertEquals(trie.size(), 2);

    upsertMetadataManager.stop();
    upsertMetadataManager.close();
  }

  @Test
  public void testAddRecordWithDeleteColumn()
      throws IOException {
    _contextBuilder.setDeleteRecordColumn(DELETE_RECORD_COLUMN);

    TriePartitionUpsertMetadataManager upsertMetadataManager =
        new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0,
            _contextBuilder.setHashFunction(HashFunction.NONE).build());
    InMemoryTrie<RecordLocation> trie = upsertMetadataManager._primaryKeyTrie;

    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds = new ThreadSafeMutableRoaringBitmap();
    MutableSegment segment = mockMutableSegment(1, validDocIds, queryableDocIds);

    // Add non-deleted record
    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 0, new IntWrapper(100), false));
    assertEquals(validDocIds.getMutableRoaringBitmap().toArray(), new int[]{0});
    assertEquals(queryableDocIds.getMutableRoaringBitmap().toArray(), new int[]{0});

    // Add deleted record for same key
    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 1, new IntWrapper(120), true));
    assertEquals(validDocIds.getMutableRoaringBitmap().toArray(), new int[]{1});
    assertEquals(queryableDocIds.getMutableRoaringBitmap().toArray(), new int[0]);

    assertEquals(trie.size(), 1);

    upsertMetadataManager.stop();
    upsertMetadataManager.close();
  }

  @Test
  public void testRemoveExpiredPrimaryKeys()
      throws IOException {
    _contextBuilder.setEnableSnapshot(true).setMetadataTTL(30);

    TriePartitionUpsertMetadataManager upsertMetadataManager =
        new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, _contextBuilder.build());
    InMemoryTrie<RecordLocation> trie = upsertMetadataManager._primaryKeyTrie;

    // Add records to update largestSeenTimestamp
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    MutableSegment segment = mockMutableSegment(1, validDocIds, null);

    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(0), 0, new Integer(80), false));
    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(1), 1, new Integer(120), false));
    upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(2), 2, new Integer(100), false));

    assertEquals(trie.size(), 3);
    assertEquals(upsertMetadataManager.getWatermark(), 120);

    // Remove expired keys (threshold = 120 - 30 = 90)
    // Key 0 (value 80) should be removed
    upsertMetadataManager.removeExpiredPrimaryKeys();

    assertEquals(trie.size(), 2);
    assertNull(trie.get(HashUtils.hashPrimaryKeyToBytes(makePrimaryKey(0), HashFunction.NONE)));
    assertNotNull(trie.get(HashUtils.hashPrimaryKeyToBytes(makePrimaryKey(1), HashFunction.NONE)));
    assertNotNull(trie.get(HashUtils.hashPrimaryKeyToBytes(makePrimaryKey(2), HashFunction.NONE)));

    upsertMetadataManager.stop();
    upsertMetadataManager.close();
  }

  @Test
  public void testHashFunction()
      throws IOException {
    // Verify that different hash functions produce correct lookups
    for (HashFunction hf : new HashFunction[]{HashFunction.NONE, HashFunction.MD5, HashFunction.MURMUR3}) {
      TriePartitionUpsertMetadataManager upsertMetadataManager =
          new TriePartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0,
              _contextBuilder.setHashFunction(hf).build());
      InMemoryTrie<RecordLocation> trie = upsertMetadataManager._primaryKeyTrie;

      ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
      MutableSegment segment = mockMutableSegment(1, validDocIds, null);

      upsertMetadataManager.addRecord(segment, new RecordInfo(makePrimaryKey(42), 0, new IntWrapper(100), false));
      assertEquals(trie.size(), 1);

      byte[] keyBytes = HashUtils.hashPrimaryKeyToBytes(makePrimaryKey(42), hf);
      RecordLocation loc = trie.get(keyBytes);
      assertNotNull(loc);
      assertSame(loc.getSegment(), segment);
      assertEquals(loc.getDocId(), 0);

      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  // ---------- Helper methods (mirror ConcurrentMap test) ----------

  private List<RecordInfo> getRecordInfoList(int numRecords, int[] primaryKeys, int[] timestamps,
      @Nullable boolean[] deleteRecordFlags) {
    List<RecordInfo> recordInfoList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      recordInfoList.add(new RecordInfo(makePrimaryKey(primaryKeys[i]), i, new IntWrapper(timestamps[i]),
          deleteRecordFlags != null && deleteRecordFlags[i]));
    }
    return recordInfoList;
  }

  private List<RecordInfo> getRecordInfoList(MutableRoaringBitmap validDocIdsSnapshot, int[] primaryKeys,
      int[] timestamps, @Nullable boolean[] deleteRecordFlags) {
    List<RecordInfo> recordInfoList = new ArrayList<>();
    Iterator<Integer> validDocIdsIterator = validDocIdsSnapshot.iterator();
    validDocIdsIterator.forEachRemaining((docId) -> recordInfoList.add(
        new RecordInfo(makePrimaryKey(primaryKeys[docId]), docId, new IntWrapper(timestamps[docId]),
            deleteRecordFlags != null && deleteRecordFlags[docId])));
    return recordInfoList;
  }

  private List<PrimaryKey> getPrimaryKeyList(int numRecords, int[] primaryKeys) {
    List<PrimaryKey> primaryKeyList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      primaryKeyList.add(makePrimaryKey(primaryKeys[i]));
    }
    return primaryKeyList;
  }

  private static ImmutableSegmentImpl mockImmutableSegment(int sequenceNumber,
      ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
      List<PrimaryKey> primaryKeys) {
    ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    when(segment.getQueryableDocIds()).thenReturn(queryableDocIds);

    DataSource primaryKeyDataSource = mock(DataSource.class);
    ForwardIndexReader primaryKeyForwardIndex = mock(ForwardIndexReader.class);
    when(primaryKeyForwardIndex.isSingleValue()).thenReturn(true);
    when(primaryKeyForwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(primaryKeyForwardIndex.createContext()).thenReturn(null);
    when(primaryKeyForwardIndex.getInt(anyInt(), any())).thenAnswer(invocation -> {
      int docId = invocation.getArgument(0);
      if (primaryKeys != null && docId < primaryKeys.size()) {
        return (Integer) primaryKeys.get(docId).getValues()[0];
      }
      return MOCK_FALLBACK_BASE_OFFSET + docId;
    });
    when(primaryKeyDataSource.getForwardIndex()).thenReturn(primaryKeyForwardIndex);

    DataSource comparisonDataSource = mock(DataSource.class);
    ForwardIndexReader comparisonForwardIndex = mock(ForwardIndexReader.class);
    when(comparisonForwardIndex.isSingleValue()).thenReturn(true);
    when(comparisonForwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(comparisonForwardIndex.createContext()).thenReturn(null);
    when(comparisonForwardIndex.getInt(anyInt(), any())).thenAnswer(invocation -> {
      int docId = invocation.getArgument(0);
      return MOCK_FALLBACK_BASE_OFFSET + (docId * 100);
    });
    when(comparisonDataSource.getForwardIndex()).thenReturn(comparisonForwardIndex);

    when(segment.getDataSource(anyString())).thenReturn(primaryKeyDataSource);
    when(segment.getDataSource(eq(PRIMARY_KEY_COLUMNS.get(0)))).thenReturn(primaryKeyDataSource);
    when(segment.getDataSource(eq(COMPARISON_COLUMNS.get(0)))).thenReturn(comparisonDataSource);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    long creationTimeMs = System.currentTimeMillis();
    when(segmentMetadata.getIndexCreationTime()).thenReturn(creationTimeMs);
    when(segmentMetadata.getZkCreationTime()).thenReturn(creationTimeMs);
    when(segmentMetadata.getTotalDocs()).thenReturn(primaryKeys != null ? primaryKeys.size() : 0);

    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    ColumnMetadata primaryKeyColumnMetadata = mock(ColumnMetadata.class);
    when(primaryKeyColumnMetadata.getFieldSpec()).thenReturn(
        new DimensionFieldSpec(PRIMARY_KEY_COLUMNS.get(0), DataType.INT, true));
    ColumnMetadata comparisonColumnMetadata = mock(ColumnMetadata.class);
    when(comparisonColumnMetadata.getFieldSpec()).thenReturn(
        new DimensionFieldSpec(COMPARISON_COLUMNS.get(0), DataType.INT, true));
    columnMetadataMap.put(PRIMARY_KEY_COLUMNS.get(0), primaryKeyColumnMetadata);
    columnMetadataMap.put(COMPARISON_COLUMNS.get(0), comparisonColumnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);

    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return segment;
  }

  private static EmptyIndexSegment mockEmptySegment(int sequenceNumber) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn(getSegmentName(sequenceNumber));
    return new EmptyIndexSegment(segmentMetadata);
  }

  private static MutableSegment mockMutableSegment(int sequenceNumber, ThreadSafeMutableRoaringBitmap validDocIds,
      ThreadSafeMutableRoaringBitmap queryableDocIds) {
    MutableSegment segment = mock(MutableSegment.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getQueryableDocIds()).thenReturn(queryableDocIds);
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    return segment;
  }

  private static String getSegmentName(int sequenceNumber) {
    return new LLCSegmentName(RAW_TABLE_NAME, 0, sequenceNumber, System.currentTimeMillis()).toString();
  }

  private static PrimaryKey makePrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static void checkRecordLocation(InMemoryTrie<RecordLocation> trie, int keyValue,
      IndexSegment segment, int docId, int comparisonValue, HashFunction hashFunction) {
    byte[] keyBytes = HashUtils.hashPrimaryKeyToBytes(makePrimaryKey(keyValue), hashFunction);
    RecordLocation recordLocation = trie.get(keyBytes);
    assertNotNull(recordLocation, "RecordLocation not found for key: " + keyValue);
    assertSame(recordLocation.getSegment(), segment);
    assertEquals(recordLocation.getDocId(), docId);
    Object actualComparisonValue = recordLocation.getComparisonValue();
    if (actualComparisonValue instanceof IntWrapper) {
      assertEquals(((IntWrapper) actualComparisonValue)._value, comparisonValue);
    } else if (actualComparisonValue instanceof Integer) {
      assertEquals(((Integer) actualComparisonValue).intValue(), comparisonValue);
    } else {
      fail("Unexpected comparison value type: " + actualComparisonValue.getClass());
    }
  }

  /**
   * Wrapper class for integer comparison values to ensure distinct references.
   */
  private static class IntWrapper implements Comparable<IntWrapper> {
    final int _value;

    IntWrapper(int value) {
      _value = value;
    }

    @Override
    public int compareTo(IntWrapper o) {
      return Integer.compare(_value, o._value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IntWrapper)) {
        return false;
      }
      IntWrapper that = (IntWrapper) o;
      return _value == that._value;
    }

    @Override
    public int hashCode() {
      return _value;
    }
  }
}
