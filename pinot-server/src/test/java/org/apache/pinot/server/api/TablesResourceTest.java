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
package org.apache.pinot.server.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TablesList;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class TablesResourceTest extends BaseResourceTest {
  @Test
  public void getTables()
      throws Exception {
    String tablesPath = "/tables";

    Response response = _webTarget.path(tablesPath).request().get(Response.class);
    String responseBody = response.readEntity(String.class);
    TablesList tablesList = JsonUtils.stringToObject(responseBody, TablesList.class);

    Assert.assertNotNull(tablesList);
    List<String> tables = tablesList.getTables();
    Assert.assertNotNull(tables);
    Assert.assertEquals(tables.size(), 2);
    Assert.assertEquals(tables.get(0), TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME));
    Assert.assertEquals(tables.get(1), TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME));

    String secondTable = "secondTable_REALTIME";
    addTable(secondTable);
    response = _webTarget.path(tablesPath).request().get(Response.class);
    responseBody = response.readEntity(String.class);
    tablesList = JsonUtils.stringToObject(responseBody, TablesList.class);

    Assert.assertNotNull(tablesList);
    tables = tablesList.getTables();
    Assert.assertNotNull(tables);
    Assert.assertEquals(tables.size(), 3);
    Assert.assertTrue(tables.contains(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME)));
    Assert.assertTrue(tables.contains(secondTable));
    Assert.assertTrue(tables.contains(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME)));
  }

  @Test
  public void getSegments()
      throws Exception {
    String segmentsPath = "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments";
    IndexSegment defaultSegment = _realtimeIndexSegments.get(0);

    TableSegments tableSegments = _webTarget.path(segmentsPath).request().get(TableSegments.class);
    Assert.assertNotNull(tableSegments);
    List<String> segmentNames = tableSegments.getSegments();
    Assert.assertNotNull(segmentNames);
    Assert.assertEquals(segmentNames.size(), 1);
    Assert.assertEquals(segmentNames.get(0), _realtimeIndexSegments.get(0).getSegmentName());

    IndexSegment secondSegment =
        setUpSegment(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), null, "0", _realtimeIndexSegments);
    tableSegments = _webTarget.path(segmentsPath).request().get(TableSegments.class);
    Assert.assertNotNull(tableSegments);
    segmentNames = tableSegments.getSegments();
    Assert.assertNotNull(segmentNames);
    Assert.assertEquals(segmentNames.size(), 2);
    Assert.assertTrue(segmentNames.contains(defaultSegment.getSegmentName()));
    Assert.assertTrue(segmentNames.contains(secondSegment.getSegmentName()));

    // No such table
    Response response = _webTarget.path("/tables/noSuchTable/segments").request().get(Response.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void getTableIndexes()
      throws Exception {
    String tableIndexesPath =
        "/tables/" + TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(TABLE_NAME) + "/indexes";

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(_webTarget.path(tableIndexesPath).request().get(String.class));
    TableIndexMetadataResponse tableIndexMetadataResponse =
        JsonUtils.jsonNodeToObject(jsonResponse, TableIndexMetadataResponse.class);
    Assert.assertNotNull(tableIndexMetadataResponse);
    Assert.assertEquals(tableIndexMetadataResponse.getTotalOnlineSegments(), _offlineIndexSegments.size());

    Map<String, Map<String, Integer>> columnToIndexCountMap = new HashMap<>();
    for (ImmutableSegment segment : _offlineIndexSegments) {
      segment.getColumnNames().forEach(colName -> {
        DataSource dataSource = segment.getDataSource(colName);
        columnToIndexCountMap.putIfAbsent(colName, new HashMap<>());
        IndexService.getInstance().getAllIndexes().forEach(indexType -> {
          int count = dataSource.getIndex(indexType) != null ? 1 : 0;
          columnToIndexCountMap.get(colName).merge(indexType.getId(), count, Integer::sum);
        });
      });
    }

    Assert.assertEquals(tableIndexMetadataResponse.getColumnToIndexesCount(), columnToIndexCountMap);

    // No such table
    Response response = _webTarget.path("/tables/noSuchTable/indexes").request().get(Response.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void getTableMetadata()
      throws Exception {
    for (TableType tableType : TableType.values()) {
      String tableMetadataPath =
          "/tables/" + TableNameBuilder.forType(tableType).tableNameWithType(TABLE_NAME) + "/metadata";

      JsonNode jsonResponse =
          JsonUtils.stringToJsonNode(_webTarget.path(tableMetadataPath).request().get(String.class));
      TableMetadataInfo metadataInfo = JsonUtils.jsonNodeToObject(jsonResponse, TableMetadataInfo.class);
      Assert.assertNotNull(metadataInfo);
      Assert.assertEquals(metadataInfo.getTableName(),
          TableNameBuilder.forType(tableType).tableNameWithType(TABLE_NAME));
      Assert.assertEquals(metadataInfo.getColumnLengthMap().size(), 0);
      Assert.assertEquals(metadataInfo.getColumnCardinalityMap().size(), 0);
      Assert.assertEquals(metadataInfo.getColumnIndexSizeMap().size(), 0);

      jsonResponse = JsonUtils.stringToJsonNode(
          _webTarget.path(tableMetadataPath).queryParam("columns", "column1").queryParam("columns", "column2").request()
              .get(String.class));
      metadataInfo = JsonUtils.jsonNodeToObject(jsonResponse, TableMetadataInfo.class);
      Assert.assertEquals(metadataInfo.getColumnLengthMap().size(), 2);
      Assert.assertEquals(metadataInfo.getColumnCardinalityMap().size(), 2);
      Assert.assertEquals(metadataInfo.getColumnIndexSizeMap().size(), 2);
      Assert.assertTrue(
          metadataInfo.getColumnIndexSizeMap().get("column1").containsKey(StandardIndexes.dictionary().getId()));
      Assert.assertTrue(
          metadataInfo.getColumnIndexSizeMap().get("column2").containsKey(StandardIndexes.forward().getId()));
    }

    // No such table
    Response response = _webTarget.path("/tables/noSuchTable/metadata").request().get(Response.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testSegmentMetadata()
      throws Exception {
    IndexSegment defaultSegment = _realtimeIndexSegments.get(0);
    String segmentMetadataPath = "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments/"
        + defaultSegment.getSegmentName() + "/metadata";

    JsonNode jsonResponse =
        JsonUtils.stringToJsonNode(_webTarget.path(segmentMetadataPath).request().get(String.class));
    SegmentMetadata segmentMetadata = defaultSegment.getSegmentMetadata();
    Assert.assertEquals(jsonResponse.get("segmentName").asText(), segmentMetadata.getName());
    Assert.assertEquals(jsonResponse.get("crc").asText(), segmentMetadata.getCrc());
    Assert.assertEquals(jsonResponse.get("creationTimeMillis").asLong(), segmentMetadata.getIndexCreationTime());
    Assert.assertTrue(jsonResponse.has("startTimeReadable"));
    Assert.assertTrue(jsonResponse.has("endTimeReadable"));
    Assert.assertTrue(jsonResponse.has("creationTimeReadable"));
    Assert.assertEquals(jsonResponse.get("columns").size(), 0);
    Assert.assertEquals(jsonResponse.get("indexes").size(), 0);

    jsonResponse = JsonUtils.stringToJsonNode(
        _webTarget.path(segmentMetadataPath).queryParam("columns", "column1").queryParam("columns", "column2").request()
            .get(String.class));
    Assert.assertEquals(jsonResponse.get("columns").size(), 2);
    Assert.assertEquals(jsonResponse.get("indexes").size(), 2);
    Assert.assertNotNull(jsonResponse.get("columns").get(0).get("indexSizeMap"));
    Assert.assertEquals(jsonResponse.get("columns").get(0).get("indexSizeMap").get("forward_index").asText(), "200008");
    Assert.assertEquals(jsonResponse.get("columns").get(0).get("indexSizeMap").get("dictionary").asText(), "206384");
    Assert.assertNotNull(jsonResponse.get("columns").get(1).get("indexSizeMap"));
    Assert.assertEquals(jsonResponse.get("columns").get(1).get("indexSizeMap").get("forward_index").asText(), "200008");
    Assert.assertEquals(jsonResponse.get("columns").get(1).get("indexSizeMap").get("dictionary").asText(), "168976");
    Assert.assertEquals(jsonResponse.get("indexes").get("column1").get("h3-index").asText(), "NO");
    Assert.assertEquals(jsonResponse.get("indexes").get("column1").get("fst-index").asText(), "NO");
    Assert.assertEquals(jsonResponse.get("indexes").get("column1").get("text-index").asText(), "NO");
    Assert.assertEquals(jsonResponse.get("indexes").get("column2").get("h3-index").asText(), "NO");
    Assert.assertEquals(jsonResponse.get("indexes").get("column2").get("fst-index").asText(), "NO");
    Assert.assertEquals(jsonResponse.get("indexes").get("column2").get("text-index").asText(), "NO");

    jsonResponse = JsonUtils.stringToJsonNode(
        (_webTarget.path(segmentMetadataPath).queryParam("columns", "*").request().get(String.class)));
    int physicalColumnCount = defaultSegment.getPhysicalColumnNames().size();
    Assert.assertEquals(jsonResponse.get("columns").size(), physicalColumnCount);
    Assert.assertEquals(jsonResponse.get("indexes").size(), physicalColumnCount);

    Response response = _webTarget.path("/tables/UNKNOWN_TABLE/segments/" + defaultSegment.getSegmentName()).request()
        .get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = _webTarget.path(
            "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments/UNKNOWN_SEGMENT")
        .request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testSegmentCrcMetadata()
      throws Exception {
    String segmentsCrcPath = "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments/crc";

    // Upload segments
    List<ImmutableSegment> immutableSegments =
        setUpSegments(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), 2, _realtimeIndexSegments);

    // Trigger crc api to fetch crc information
    String response = _webTarget.path(segmentsCrcPath).request().get(String.class);
    JsonNode segmentsCrc = JsonUtils.stringToJsonNode(response);

    // Check that crc info is correct
    for (ImmutableSegment immutableSegment : immutableSegments) {
      String segmentName = immutableSegment.getSegmentName();
      String crc = immutableSegment.getSegmentMetadata().getCrc();
      Assert.assertEquals(segmentsCrc.get(segmentName).asText(), crc);
    }
  }

  @Test
  public void testDownloadSegments()
      throws Exception {
    // Verify the content of the downloaded segment from a realtime table.
    downLoadAndVerifySegmentContent(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        _realtimeIndexSegments.get(0));
    // Verify the content of the downloaded segment from an offline table.
    downLoadAndVerifySegmentContent(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
        _offlineIndexSegments.get(0));

    // Verify non-existent table and segment download return NOT_FOUND status.
    Response response = _webTarget.path("/tables/UNKNOWN_REALTIME/segments/segmentname").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = _webTarget.path(
            "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments/UNKNOWN_SEGMENT")
        .request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testDownloadValidDocIdsSnapshot()
      throws Exception {
    // Verify the content of the downloaded snapshot from a realtime table.
    downLoadAndVerifyValidDocIdsSnapshot(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) _realtimeIndexSegments.get(0));
    downLoadAndVerifyValidDocIdsSnapshotBitmap(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) _realtimeIndexSegments.get(0));

    // Verify non-existent table and segment download return NOT_FOUND status.
    Response response =
        _webTarget.path("/tables/UNKNOWN_REALTIME/segments/segmentname/validDocIds").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = _webTarget.path(
        String.format("/tables/%s/segments/%s/validDocIds", TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
            "UNKNOWN_SEGMENT")).request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Deprecated
  @Test
  public void testValidDocIdMetadata()
      throws IOException {
    IndexSegment segment = _realtimeIndexSegments.get(0);
    // Verify the content of the downloaded snapshot from a realtime table.
    downLoadAndVerifyValidDocIdsSnapshot(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) segment);
    downLoadAndVerifyValidDocIdsSnapshotBitmap(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) segment);

    String validDocIdMetadataPath =
        "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/validDocIdMetadata";
    String metadataResponse =
        _webTarget.path(validDocIdMetadataPath).queryParam("segmentNames", segment.getSegmentName()).request()
            .get(String.class);
    JsonNode validDocIdMetadata = JsonUtils.stringToJsonNode(metadataResponse).get(0);

    Assert.assertEquals(validDocIdMetadata.get("totalDocs").asInt(), 100000);
    Assert.assertEquals(validDocIdMetadata.get("totalValidDocs").asInt(), 8);
    Assert.assertEquals(validDocIdMetadata.get("totalInvalidDocs").asInt(), 99992);
    Assert.assertEquals(validDocIdMetadata.get("segmentCrc").asText(), "1894900283");
    Assert.assertEquals(validDocIdMetadata.get("validDocIdsType").asText(), "SNAPSHOT");
  }

  @Test
  public void testValidDocIdsMetadataPost()
      throws IOException {
    IndexSegment segment = _realtimeIndexSegments.get(0);
    // Verify the content of the downloaded snapshot from a realtime table.
    downLoadAndVerifyValidDocIdsSnapshot(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) segment);
    downLoadAndVerifyValidDocIdsSnapshotBitmap(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        (ImmutableSegmentImpl) segment);

    List<String> segments = List.of(segment.getSegmentName());
    TableSegments tableSegments = new TableSegments(segments);
    String validDocIdsMetadataPath =
        "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/validDocIdsMetadata";
    String response =
        _webTarget.path(validDocIdsMetadataPath).queryParam("segmentNames", segment.getSegmentName()).request()
            .post(Entity.json(tableSegments), String.class);
    JsonNode validDocIdsMetadata = JsonUtils.stringToJsonNode(response).get(0);

    Assert.assertEquals(validDocIdsMetadata.get("totalDocs").asInt(), 100000);
    Assert.assertEquals(validDocIdsMetadata.get("totalValidDocs").asInt(), 8);
    Assert.assertEquals(validDocIdsMetadata.get("totalInvalidDocs").asInt(), 99992);
    Assert.assertEquals(validDocIdsMetadata.get("segmentCrc").asText(), "1894900283");
    Assert.assertEquals(validDocIdsMetadata.get("validDocIdsType").asText(), "SNAPSHOT");
    Assert.assertEquals(validDocIdsMetadata.get("segmentSizeInBytes").asLong(), 1877636);
    Assert.assertTrue(validDocIdsMetadata.has("segmentCreationTimeMillis"));
    Assert.assertTrue(validDocIdsMetadata.get("segmentCreationTimeMillis").asLong() > 0);

    // Verify server status information
    Assert.assertTrue(validDocIdsMetadata.has("serverStatus"), "Server status should be included in response");
    String serverStatus = validDocIdsMetadata.get("serverStatus").asText();
    Assert.assertNotNull(serverStatus, "Server status should not be null");
    Assert.assertEquals(serverStatus, "NOT_STARTED", serverStatus);
  }

  // Verify metadata file from segments.
  private void downLoadAndVerifySegmentContent(String tableNameWithType, IndexSegment segment)
      throws IOException, ConfigurationException {
    String segmentPath = "/segments/" + tableNameWithType + "/" + segment.getSegmentName();

    // Download the segment and save to a temp local file.
    Response response = _webTarget.path(segmentPath).request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    File segmentFile = response.readEntity(File.class);

    File tempMetadataDir = new File(FileUtils.getTempDirectory(), "segment_metadata");
    FileUtils.forceMkdir(tempMetadataDir);

    // Extract metadata.properties
    TarCompressionUtils.untarOneFile(segmentFile, V1Constants.MetadataKeys.METADATA_FILE_NAME,
        new File(tempMetadataDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    // Extract creation.meta
    TarCompressionUtils.untarOneFile(segmentFile, V1Constants.SEGMENT_CREATION_META,
        new File(tempMetadataDir, V1Constants.SEGMENT_CREATION_META));

    // Load segment metadata
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(tempMetadataDir);
    Assert.assertEquals(metadata.getTableName(), TableNameBuilder.extractRawTableName(tableNameWithType));

    FileUtils.forceDelete(tempMetadataDir);
  }

  // Verify metadata file from segments.
  private void downLoadAndVerifyValidDocIdsSnapshot(String tableNameWithType, ImmutableSegmentImpl segment)
      throws IOException {
    String snapshotPath = "/segments/" + tableNameWithType + "/" + segment.getSegmentName() + "/validDocIds";

    PartitionUpsertMetadataManager upsertMetadataManager = mock(PartitionUpsertMetadataManager.class);
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap validDocIdsSnapshot = new ThreadSafeMutableRoaringBitmap();

    int[] docIds = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    for (int docId : docIds) {
      validDocIds.add(docId);
      queryableDocIds.add(docId + 1);
      validDocIdsSnapshot.add(docId + 2);
    }
    segment.enableUpsert(upsertMetadataManager, validDocIds, queryableDocIds);
    File validDocIdsSnapshotFile =
        new File(SegmentDirectoryPaths.findSegmentDirectory(segment.getSegmentMetadata().getIndexDir()),
            V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
    FileUtils.writeByteArrayToFile(validDocIdsSnapshotFile,
        RoaringBitmapUtils.serialize(validDocIdsSnapshot.getMutableRoaringBitmap()));

    // Check no type (default should be validDocIdsSnapshot)
    Response response = _webTarget.path(snapshotPath).request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    byte[] validDocIdsSnapshotBitmap = response.readEntity(byte[].class);
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIdsSnapshot.getMutableRoaringBitmap());

    // Check snapshot type
    response =
        _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.SNAPSHOT.toString()).request()
            .get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    validDocIdsSnapshotBitmap = response.readEntity(byte[].class);
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIdsSnapshot.getMutableRoaringBitmap());

    // Check onHeap type
    response = _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.IN_MEMORY).request()
        .get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    validDocIdsSnapshotBitmap = response.readEntity(byte[].class);
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIds.getMutableRoaringBitmap());

    // Check onHeapWithDelete type
    response =
        _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.IN_MEMORY_WITH_DELETE.toString())
            .request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    validDocIdsSnapshotBitmap = response.readEntity(byte[].class);
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        queryableDocIds.getMutableRoaringBitmap());
  }

  private void downLoadAndVerifyValidDocIdsSnapshotBitmap(String tableNameWithType, ImmutableSegmentImpl segment)
      throws IOException {
    String snapshotPath = "/segments/" + tableNameWithType + "/" + segment.getSegmentName() + "/validDocIdsBitmap";

    PartitionUpsertMetadataManager upsertMetadataManager = mock(PartitionUpsertMetadataManager.class);
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap validDocIdsSnapshot = new ThreadSafeMutableRoaringBitmap();

    int[] docIds = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    for (int docId : docIds) {
      validDocIds.add(docId);
      queryableDocIds.add(docId + 1);
      validDocIdsSnapshot.add(docId + 2);
    }
    segment.enableUpsert(upsertMetadataManager, validDocIds, queryableDocIds);
    File validDocIdsSnapshotFile =
        new File(SegmentDirectoryPaths.findSegmentDirectory(segment.getSegmentMetadata().getIndexDir()),
            V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
    FileUtils.writeByteArrayToFile(validDocIdsSnapshotFile,
        RoaringBitmapUtils.serialize(validDocIdsSnapshot.getMutableRoaringBitmap()));
    String expectedSegmentCrc = "1894900283";

    // Check no type (default should be validDocIdsSnapshot)
    ValidDocIdsBitmapResponse response = _webTarget.path(snapshotPath).request().get(ValidDocIdsBitmapResponse.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getSegmentCrc(), expectedSegmentCrc);
    Assert.assertEquals(response.getSegmentName(), segment.getSegmentName());
    byte[] validDocIdsSnapshotBitmap = response.getBitmap();
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIdsSnapshot.getMutableRoaringBitmap());

    // Check snapshot type
    response =
        _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.SNAPSHOT.toString()).request()
            .get(ValidDocIdsBitmapResponse.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getSegmentCrc(), expectedSegmentCrc);
    Assert.assertEquals(response.getSegmentName(), segment.getSegmentName());
    validDocIdsSnapshotBitmap = response.getBitmap();
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIdsSnapshot.getMutableRoaringBitmap());

    // Check onHeap type
    response =
        _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.IN_MEMORY.toString()).request()
            .get(ValidDocIdsBitmapResponse.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getSegmentCrc(), expectedSegmentCrc);
    Assert.assertEquals(response.getSegmentName(), segment.getSegmentName());
    validDocIdsSnapshotBitmap = response.getBitmap();
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        validDocIds.getMutableRoaringBitmap());

    // Check onHeapWithDelete type
    response =
        _webTarget.path(snapshotPath).queryParam("validDocIdsType", ValidDocIdsType.IN_MEMORY_WITH_DELETE.toString())
            .request().get(ValidDocIdsBitmapResponse.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getSegmentCrc(), expectedSegmentCrc);
    Assert.assertEquals(response.getSegmentName(), segment.getSegmentName());
    validDocIdsSnapshotBitmap = response.getBitmap();
    Assert.assertNotNull(validDocIdsSnapshotBitmap);
    Assert.assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(validDocIdsSnapshotBitmap)).toMutableRoaringBitmap(),
        queryableDocIds.getMutableRoaringBitmap());
  }

  @Test
  public void testUploadSegments()
      throws Exception {
    setUpSegment(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS, null,
        _realtimeIndexSegments);
    setUpSegment(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), LLC_SEGMENT_NAME_FOR_UPLOAD_FAILURE, null,
        _realtimeIndexSegments);

    // Verify segment uploading succeed.
    Response response = _webTarget.path(
        String.format("/segments/%s/%s/upload", TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
            LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS)).request().post(null);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(response.readEntity(String.class), SEGMENT_DOWNLOAD_URL);

    // Verify bad request: table type is offline
    response = _webTarget.path(
        String.format("/segments/%s/%s/upload", TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
            _offlineIndexSegments.get(0).getSegmentName())).request().post(null);
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Verify bad request: segment is not low level consumer segment
    response = _webTarget.path(
        String.format("/segments/%s/%s/upload", TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
            _realtimeIndexSegments.get(0).getSegmentName())).request().post(null);
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Verify non-existent segment uploading fail with NOT_FOUND status.
    response =
        _webTarget.path(String.format("/segments/%s/%s_dummy/upload", TABLE_NAME, LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS))
            .request().post(null);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    // Verify fail to upload segment to segment store with internal server error.
    response = _webTarget.path(String.format("/segments/%s/%s/upload", TABLE_NAME, LLC_SEGMENT_NAME_FOR_UPLOAD_FAILURE))
        .request().post(null);
    Assert.assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }

  @Test
  public void testOfflineTableSegmentMetadata()
      throws Exception {
    IndexSegment defaultSegment = _offlineIndexSegments.get(0);
    String segmentMetadataPath = "/tables/" + TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME) + "/segments/"
        + defaultSegment.getSegmentName() + "/metadata";

    JsonNode jsonResponse =
        JsonUtils.stringToJsonNode(_webTarget.path(segmentMetadataPath).request().get(String.class));

    SegmentMetadata segmentMetadata = defaultSegment.getSegmentMetadata();
    Assert.assertEquals(jsonResponse.get("segmentName").asText(), segmentMetadata.getName());
    Assert.assertEquals(jsonResponse.get("crc").asText(), segmentMetadata.getCrc());
    Assert.assertEquals(jsonResponse.get("creationTimeMillis").asLong(), segmentMetadata.getIndexCreationTime());
    Assert.assertTrue(jsonResponse.has("startTimeReadable"));
    Assert.assertTrue(jsonResponse.has("endTimeReadable"));
    Assert.assertTrue(jsonResponse.has("creationTimeReadable"));
    Assert.assertEquals(jsonResponse.get("columns").size(), 0);
    Assert.assertEquals(jsonResponse.get("indexes").size(), 0);

    jsonResponse = JsonUtils.stringToJsonNode(
        _webTarget.path(segmentMetadataPath).queryParam("columns", "column1").queryParam("columns", "column2").request()
            .get(String.class));
    Assert.assertEquals(jsonResponse.get("columns").size(), 2);
    Assert.assertEquals(jsonResponse.get("indexes").size(), 2);
    Assert.assertEquals(jsonResponse.get("star-tree-index").size(), 0);

    jsonResponse = JsonUtils.stringToJsonNode(
        (_webTarget.path(segmentMetadataPath).queryParam("columns", "*").request().get(String.class)));
    int physicalColumnCount = defaultSegment.getPhysicalColumnNames().size();
    Assert.assertEquals(jsonResponse.get("columns").size(), physicalColumnCount);
    Assert.assertEquals(jsonResponse.get("indexes").size(), physicalColumnCount);

    Response response = _webTarget.path("/tables/UNKNOWN_TABLE/segments/" + defaultSegment.getSegmentName()).request()
        .get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = _webTarget.path(
            "/tables/" + TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME) + "/segments/UNKNOWN_SEGMENT")
        .request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }
}
