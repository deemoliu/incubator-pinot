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
package org.apache.pinot.segment.local.segment.creator;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class TransformPipelineTest {

  private static TableConfig createTestTableConfig()
      throws Exception {
    return Fixtures.createTableConfig("some.consumer.class", "some.decoder.class");
  }

  @Test
  public void testSingleRow()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow simpleRow = Fixtures.createSingleRow(9527);
    TransformPipeline.Result result = pipeline.processRow(simpleRow);
    assertNotNull(result);
    assertEquals(result.getTransformedRows().size(), 1);
    assertEquals(result.getSkippedRowCount(), 0);
    assertEquals(result.getTransformedRows().get(0), simpleRow);
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Caught exception while transforming data type.*")
  public void testSingleRowFailure()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow simpleRow = Fixtures.createInvalidSingleRow(9527);
    pipeline.processRow(simpleRow);
  }

  @Test
  public void testMultipleRow()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow multipleRow = Fixtures.createMultipleRow(9527);
    //noinspection unchecked
    List<GenericRow> rows = (List<GenericRow>) multipleRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    TransformPipeline.Result result = pipeline.processRow(multipleRow);
    assertNotNull(result);
    assertEquals(result.getTransformedRows().size(), rows.size());
    assertEquals(result.getSkippedRowCount(), 0);
    assertEquals(result.getTransformedRows(), rows);
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Caught exception while transforming data type.*")
  public void testMultipleRowPartialFailure()
      throws Exception {
    TableConfig config = createTestTableConfig();
    Schema schema = Fixtures.createSchema();
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow multipleRow = Fixtures.createMultipleRowPartialFailure(9527);
    pipeline.processRow(multipleRow);
  }

  @Test
  public void testUnnestFieldWithTransform()
      throws Exception {
    TableConfig config = JsonUtils.stringToObject(
        "{\n"
            + "  \"tableName\": \"githubComplexTypeEvents\",\n"
            + "  \"tableType\": \"OFFLINE\",\n"
            + "  \"tenants\": {\n"
            + "  },\n"
            + "  \"segmentsConfig\": {\n"
            + "    \"segmentPushType\": \"REFRESH\",\n"
            + "    \"replication\": \"1\",\n"
            + "    \"timeColumnName\": \"created_at_timestamp\"\n"
            + "  },\n"
            + "  \"tableIndexConfig\": {\n"
            + "    \"loadMode\": \"MMAP\"\n"
            + "  },\n"
            + "  \"ingestionConfig\": {\n"
            + "    \"transformConfigs\": [\n"
            + "      {\n"
            + "        \"columnName\": \"created_at_timestamp\",\n"
            + "        \"transformFunction\": \"fromDateTime(created_at, 'yyyy-MM-dd''T''HH:mm:ss''Z''')\"\n"
            + "      }\n"
            + "    ],\n"
            + "    \"complexTypeConfig\": {\n"
            + "      \"fieldsToUnnest\": [\n"
            + "        \"payload.commits\"\n"
            + "      ],\n"
            + "      \"prefixesToRename\": {\n"
            + "        \"payload.\": \"\"\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"metadata\": {\n"
            + "    \"customConfigs\": {\n"
            + "    }\n"
            + "  }\n"
            + "}\n", TableConfig.class);
    Schema schema = Schema.fromString(
        "{\n"
            + "  \"dimensionFieldSpecs\": [\n"
            + "    {\n"
            + "      \"name\": \"id\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"type\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"push_id\",\n"
            + "      \"dataType\": \"LONG\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"size\",\n"
            + "      \"dataType\": \"INT\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"distinct_size\",\n"
            + "      \"dataType\": \"INT\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"ref\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"head\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"before\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.sha\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.author.name\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.author.email\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.message\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.distinct\",\n"
            + "      \"dataType\": \"BOOLEAN\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.url\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"dateTimeFieldSpecs\": [\n"
            + "    {\n"
            + "      \"name\": \"created_at\",\n"
            + "      \"dataType\": \"STRING\",\n"
            + "      \"format\": \"1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss'Z'\",\n"
            + "      \"granularity\": \"1:SECONDS\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"created_at_timestamp\",\n"
            + "      \"dataType\": \"TIMESTAMP\",\n"
            + "      \"format\": \"1:MILLISECONDS:TIMESTAMP\",\n"
            + "      \"granularity\": \"1:SECONDS\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"schemaName\": \"githubComplexTypeEvents\"\n"
            + "}\n");
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow sampleRow = new GenericRow();
    sampleRow.putValue("id", "7044874109");
    sampleRow.putValue("type", "PushEvent");
    sampleRow.putValue("actor", Map.of(
        "id", 18542751,
        "login", "LimeVista",
        "display_login", "LimeVista",
        "gravatar_id", "",
        "url", "https://api.github.com/users/LimeVista",
        "avatar_url", "https://avatars.githubusercontent.com/u/18542751?"
    ));
    sampleRow.putValue("repo", Map.of(
        "id", 115911530,
        "name", "LimeVista/Tapes",
        "url", "https://api.github.com/repos/LimeVista/Tapes"
    ));
    sampleRow.putValue("payload", Map.of(
        "push_id", "2226018068",
        "size", 1,
        "distinct_size", 1,
        "ref", "refs/heads/master",
        "head", "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774",
        "before", "892d872c5d3f24cc6837900c9f4618dc2fe92930",
        "commits", Map.of(
            "sha", "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774",
            "author", Map.of(
                "name", "Lime",
                "email", "4cc153d999e24274955157fc813e6f92f821525d@outlook.com"),
            "message", "Merge branch 'master' of https://github.com/LimeVista/Tapes\\n\\n# Conflicts:\\n#\\t.gitignore",
            "distinct", true,
            "url", "https://api.github.com/repos/LimeVista/Tapes/commits/c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774"
        )
    ));
    sampleRow.putValue("public", true);
    sampleRow.putValue("created_at", "2018-01-01T11:00:00Z");

    TransformPipeline.Result result = pipeline.processRow(sampleRow);
    List<GenericRow> transformedRows = result.getTransformedRows();
    assertEquals(transformedRows.size(), 1);
    GenericRow transformedRow = transformedRows.get(0);
    assertEquals(transformedRow.getValue("created_at_timestamp"), 1514804400000L);
    assertEquals(transformedRow.getValue("commits.author.email"),
        "4cc153d999e24274955157fc813e6f92f821525d@outlook.com");
    assertEquals(transformedRow.getValue("commits.author.name"), "Lime");
    assertEquals(transformedRow.getValue("commits.message"),
        "Merge branch 'master' of https://github.com/LimeVista/Tapes\\n\\n# Conflicts:\\n#\\t.gitignore");
    assertEquals(transformedRow.getValue("commits.sha"), "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("commits.distinct"), 1);
    assertEquals(transformedRow.getValue("commits.url"),
        "https://api.github.com/repos/LimeVista/Tapes/commits/c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("ref"), "refs/heads/master");
    assertEquals(transformedRow.getValue("distinct_size"), 1);
    assertEquals(transformedRow.getValue("head"), "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("push_id"), 2226018068L);
    assertEquals(transformedRow.getValue("size"), 1);
    assertEquals(transformedRow.getValue("before"), "892d872c5d3f24cc6837900c9f4618dc2fe92930");
  }

  @Test
  public void testRenameFieldWithTransform()
      throws Exception {
    TableConfig config = JsonUtils.stringToObject(
        "{\n"
            + "  \"tableName\": \"githubComplexTypeEvents\",\n"
            + "  \"tableType\": \"OFFLINE\",\n"
            + "  \"tenants\": {\n"
            + "  },\n"
            + "  \"segmentsConfig\": {\n"
            + "    \"segmentPushType\": \"REFRESH\",\n"
            + "    \"replication\": \"1\",\n"
            + "    \"timeColumnName\": \"created_at_timestamp\"\n"
            + "  },\n"
            + "  \"tableIndexConfig\": {\n"
            + "    \"loadMode\": \"MMAP\"\n"
            + "  },\n"
            + "  \"ingestionConfig\": {\n"
            + "    \"transformConfigs\": [\n"
            + "      {\n"
            + "        \"columnName\": \"created_at_timestamp\",\n"
            + "        \"transformFunction\": \"fromDateTime(created_at, 'yyyy-MM-dd''T''HH:mm:ss''Z''')\"\n"
            + "      }\n"
            + "    ],\n"
            + "    \"complexTypeConfig\": {\n"
            + "      \"prefixesToRename\": {\n"
            + "        \"payload.\": \"\",\n"
            + "        \"actor.\": \"a.\",\n"
            + "        \"repo.\": \"r.\"\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"metadata\": {\n"
            + "    \"customConfigs\": {\n"
            + "    }\n"
            + "  }\n"
            + "}\n", TableConfig.class);
    Schema schema = Schema.fromString(
        "{\n"
            + "  \"dimensionFieldSpecs\": [\n"
            + "    {\n"
            + "      \"name\": \"id\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"type\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"push_id\",\n"
            + "      \"dataType\": \"LONG\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"size\",\n"
            + "      \"dataType\": \"INT\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"distinct_size\",\n"
            + "      \"dataType\": \"INT\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"ref\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"head\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"before\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.sha\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.author.name\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.author.email\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.message\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.distinct\",\n"
            + "      \"dataType\": \"BOOLEAN\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"commits.url\",\n"
            + "      \"dataType\": \"STRING\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"dateTimeFieldSpecs\": [\n"
            + "    {\n"
            + "      \"name\": \"created_at\",\n"
            + "      \"dataType\": \"STRING\",\n"
            + "      \"format\": \"1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss'Z'\",\n"
            + "      \"granularity\": \"1:SECONDS\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"created_at_timestamp\",\n"
            + "      \"dataType\": \"TIMESTAMP\",\n"
            + "      \"format\": \"1:MILLISECONDS:TIMESTAMP\",\n"
            + "      \"granularity\": \"1:SECONDS\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"schemaName\": \"githubComplexTypeEvents\"\n"
            + "}\n");
    TransformPipeline pipeline = new TransformPipeline(config, schema);
    GenericRow sampleRow = new GenericRow();
    sampleRow.putValue("id", "7044874109");
    sampleRow.putValue("type", "PushEvent");
    sampleRow.putValue("actor", Map.of(
        "id", 18542751,
        "login", "LimeVista",
        "display_login", "LimeVista",
        "gravatar_id", "",
        "url", "https://api.github.com/users/LimeVista",
        "avatar_url", "https://avatars.githubusercontent.com/u/18542751?"
    ));

    sampleRow.putValue("repo", Map.of(
        "id", 115911530,
        "name", "LimeVista/Tapes",
        "url", "https://api.github.com/repos/LimeVista/Tapes"
    ));
    sampleRow.putValue("payload", Map.of(
        "push_id", "2226018068",
        "size", 1,
        "distinct_size", 1,
        "ref", "refs/heads/master",
        "head", "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774",
        "before", "892d872c5d3f24cc6837900c9f4618dc2fe92930",
        "commits", Map.of(
            "sha", "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774",
            "author", Map.of(
                "name", "Lime",
                "email", "4cc153d999e24274955157fc813e6f92f821525d@outlook.com"),
            "message", "Merge branch 'master' of https://github.com/LimeVista/Tapes\\n\\n# Conflicts:\\n#\\t.gitignore",
            "distinct", true,
            "url", "https://api.github.com/repos/LimeVista/Tapes/commits/c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774"
        )
    ));
    sampleRow.putValue("public", true);
    sampleRow.putValue("created_at", "2018-01-01T11:00:00Z");

    TransformPipeline.Result result = pipeline.processRow(sampleRow);
    List<GenericRow> transformedRows = result.getTransformedRows();
    assertEquals(transformedRows.size(), 1);
    GenericRow transformedRow = transformedRows.get(0);
    assertEquals(transformedRow.getValue("created_at_timestamp"), 1514804400000L);
    assertEquals(transformedRow.getValue("commits.author.email"),
        "4cc153d999e24274955157fc813e6f92f821525d@outlook.com");
    assertEquals(transformedRow.getValue("commits.author.name"), "Lime");
    assertEquals(transformedRow.getValue("commits.message"),
        "Merge branch 'master' of https://github.com/LimeVista/Tapes\\n\\n# Conflicts:\\n#\\t.gitignore");
    assertEquals(transformedRow.getValue("commits.sha"), "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("commits.distinct"), 1);
    assertEquals(transformedRow.getValue("commits.url"),
        "https://api.github.com/repos/LimeVista/Tapes/commits/c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("ref"), "refs/heads/master");
    assertEquals(transformedRow.getValue("distinct_size"), 1);
    assertEquals(transformedRow.getValue("head"), "c5fc8b32a9ead1eba315d97410cb4ac1e6ca1774");
    assertEquals(transformedRow.getValue("push_id"), 2226018068L);
    assertEquals(transformedRow.getValue("size"), 1);
    assertEquals(transformedRow.getValue("before"), "892d872c5d3f24cc6837900c9f4618dc2fe92930");

    assertEquals(transformedRow.getValue("a.id"), 18542751);
    assertEquals(transformedRow.getValue("a.login"), "LimeVista");
    assertEquals(transformedRow.getValue("a.display_login"), "LimeVista");
    assertEquals(transformedRow.getValue("a.gravatar_id"), "");
    assertEquals(transformedRow.getValue("a.url"), "https://api.github.com/users/LimeVista");
    assertEquals(transformedRow.getValue("a.avatar_url"), "https://avatars.githubusercontent.com/u/18542751?");

    assertEquals(transformedRow.getValue("r.id"), 115911530);
    assertEquals(transformedRow.getValue("r.name"), "LimeVista/Tapes");
    assertEquals(transformedRow.getValue("r.url"), "https://api.github.com/repos/LimeVista/Tapes");
  }
}
