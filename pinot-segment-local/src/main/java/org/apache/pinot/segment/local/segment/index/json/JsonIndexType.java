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

package org.apache.pinot.segment.local.segment.index.json;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


public class JsonIndexType extends AbstractIndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "json";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);

  protected JsonIndexType() {
    super(StandardIndexes.JSON_ID);
  }

  @Override
  public Class<JsonIndexConfig> getIndexConfigClass() {
    return JsonIndexConfig.class;
  }

  @Override
  public JsonIndexConfig getDefaultConfig() {
    return JsonIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    JsonIndexConfig jsonIndexConfig = indexConfigs.getConfig(StandardIndexes.json());
    if (jsonIndexConfig.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot create JSON index on multi-value column: %s",
          column);
      DataType storedType = fieldSpec.getDataType().getStoredType();
      Preconditions.checkState(storedType == DataType.STRING || storedType == DataType.MAP,
          "Cannot create JSON index on column: %s of stored type other than STRING or MAP", column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<JsonIndexConfig> createDeserializerForLegacyConfigs() {
    ColumnConfigDeserializer<JsonIndexConfig> fromJsonIndexConfigs =
        IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getJsonIndexConfigs());
    ColumnConfigDeserializer<JsonIndexConfig> fromJsonIndexColumns =
        IndexConfigDeserializer.fromCollection(tableConfig -> tableConfig.getIndexingConfig().getJsonIndexColumns(),
            (accum, column) -> accum.put(column, JsonIndexConfig.DEFAULT));
    ColumnConfigDeserializer<JsonIndexConfig> fromFieldConfigs =
        IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.JSON,
            (tableConfig, fieldConfig) -> JsonIndexConfig.DEFAULT);
    return fromJsonIndexConfigs.withFallbackAlternative(fromJsonIndexColumns).withFallbackAlternative(fromFieldConfigs);
  }

  @Override
  public JsonIndexCreator createIndexCreator(IndexCreationContext context, JsonIndexConfig indexConfig)
      throws IOException {
    DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "Json index is currently only supported on single-value columns");
    Preconditions.checkState(storedType == DataType.STRING || storedType == DataType.MAP,
        "Json index is currently only supported on STRING columns");
    return context.isOnHeap() ? new OnHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
        context.getTableNameWithType(), context.isContinueOnError(), indexConfig)
        : new OffHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
            context.getTableNameWithType(), context.isContinueOnError(), indexConfig);
  }

  @Override
  protected IndexReaderFactory<JsonIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  public static JsonIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.createIndexReader(dataBuffer, columnMetadata);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return new JsonIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<JsonIndexConfig, JsonIndexReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<JsonIndexConfig, JsonIndexReader, ?> getIndexType() {
      return StandardIndexes.json();
    }

    @Override
    protected JsonIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        JsonIndexConfig indexConfig)
        throws IndexReaderConstraintException {
      return createIndexReader(dataBuffer, metadata);
    }

    public static JsonIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (!metadata.getFieldSpec().isSingleValueField()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Json index is currently only supported on single-value columns");
      }
      DataType storedType = metadata.getFieldSpec().getDataType().getStoredType();
      if (storedType != DataType.STRING && storedType != DataType.MAP) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Json index is currently only supported on STRING columns");
      }
      return new ImmutableJsonIndexReader(dataBuffer, metadata.getTotalDocs());
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setJsonIndexColumns(null);
    tableConfig.getIndexingConfig().setJsonIndexConfigs(null);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, JsonIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().isSingleValueField()) {
      return null;
    }
    return new MutableJsonIndexImpl(config, context.getSegmentName(), context.getFieldSpec().getName());
  }
}
