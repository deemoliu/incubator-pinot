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
package org.apache.pinot.core.operator.blocks.results;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.ArrayListUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.Table;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Results block for group-by queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByResultsBlock extends BaseResultsBlock {
  private final DataSchema _dataSchema;
  private final AggregationGroupByResult _aggregationGroupByResult;
  private final Collection<IntermediateRecord> _intermediateRecords;
  private final Table _table;
  private final QueryContext _queryContext;

  private boolean _groupsTrimmed;
  private boolean _numGroupsLimitReached;
  private boolean _numGroupsWarningLimitReached;
  private int _numResizes;
  private long _resizeTimeMs;

  /**
   * For segment level group-by results.
   */
  public GroupByResultsBlock(DataSchema dataSchema, AggregationGroupByResult aggregationGroupByResult,
      QueryContext queryContext) {
    _dataSchema = dataSchema;
    _aggregationGroupByResult = aggregationGroupByResult;
    _intermediateRecords = null;
    _table = null;
    _queryContext = queryContext;
  }

  /**
   * For segment level group-by results.
   */
  public GroupByResultsBlock(DataSchema dataSchema, Collection<IntermediateRecord> intermediateRecords,
      QueryContext queryContext) {
    _dataSchema = dataSchema;
    _aggregationGroupByResult = null;
    _intermediateRecords = intermediateRecords;
    _table = null;
    _queryContext = queryContext;
  }

  /**
   * For instance level group-by results.
   */
  public GroupByResultsBlock(Table table, QueryContext queryContext) {
    _dataSchema = table.getDataSchema();
    _aggregationGroupByResult = null;
    _intermediateRecords = null;
    _table = table;
    _queryContext = queryContext;
  }

  /**
   * For instance level empty group-by results.
   */
  public GroupByResultsBlock(DataSchema dataSchema, QueryContext queryContext) {
    _dataSchema = dataSchema;
    _aggregationGroupByResult = null;
    _intermediateRecords = null;
    _table = null;
    _queryContext = queryContext;
  }

  public AggregationGroupByResult getAggregationGroupByResult() {
    return _aggregationGroupByResult;
  }

  public Collection<IntermediateRecord> getIntermediateRecords() {
    return _intermediateRecords;
  }

  public Table getTable() {
    return _table;
  }

  public int getNumGroups() {
    assert _aggregationGroupByResult != null || _intermediateRecords != null
        : "Should not call getNumGroups() on instance level results";
    if (_aggregationGroupByResult != null) {
      return _aggregationGroupByResult.getNumGroups();
    } else {
      return _intermediateRecords.size();
    }
  }

  public boolean isNumGroupsLimitReached() {
    return _numGroupsLimitReached;
  }

  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _numGroupsLimitReached = numGroupsLimitReached;
  }

  public boolean isGroupsTrimmed() {
    return _groupsTrimmed;
  }

  public void setGroupsTrimmed(boolean groupsTrimmed) {
    _groupsTrimmed = groupsTrimmed;
  }

  public boolean isNumGroupsWarningLimitReached() {
    return _numGroupsWarningLimitReached;
  }

  public void setNumGroupsWarningLimitReached(boolean numGroupsWarningLimitReached) {
    _numGroupsWarningLimitReached = numGroupsWarningLimitReached;
  }

  public int getNumResizes() {
    return _numResizes;
  }

  public void setNumResizes(int numResizes) {
    _numResizes = numResizes;
  }

  public long getResizeTimeMs() {
    return _resizeTimeMs;
  }

  public void setResizeTimeMs(long resizeTimeMs) {
    _resizeTimeMs = resizeTimeMs;
  }

  @Override
  public int getNumRows() {
    return _table == null ? 0 : _table.size();
  }

  @Override
  public QueryContext getQueryContext() {
    return _queryContext;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public List<Object[]> getRows() {
    if (_table == null) {
      return Collections.emptyList();
    }
    List<Object[]> rows = new ArrayList<>(_table.size());
    Iterator<Record> iterator = _table.iterator();
    while (iterator.hasNext()) {
      rows.add(iterator.next().getValues());
    }
    return rows;
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(_dataSchema);
    if (_table == null) {
      return dataTableBuilder.build();
    }
    ColumnDataType[] storedColumnDataTypes = _dataSchema.getStoredColumnDataTypes();
    int numColumns = _dataSchema.size();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    List<ExpressionContext> groupByExpressions = _queryContext.getGroupByExpressions();
    assert aggregationFunctions != null && groupByExpressions != null;
    int numKeyColumns = groupByExpressions.size();
    Iterator<Record> iterator = _table.iterator();
    int numRowsAdded = 0;
    if (_queryContext.isNullHandlingEnabled()) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      Object[] nullPlaceholders = new Object[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        nullBitmaps[colId] = new RoaringBitmap();
        nullPlaceholders[colId] = storedColumnDataTypes[colId].getNullPlaceholder();
      }
      int rowId = 0;
      while (iterator.hasNext()) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
        dataTableBuilder.startRow();
        Object[] values = iterator.next().getValues();
        for (int i = 0; i < numColumns; i++) {
          Object value = values[i];
          if (storedColumnDataTypes[i] == ColumnDataType.OBJECT) {
            if (value == null) {
              dataTableBuilder.setNull(i);
            } else {
              dataTableBuilder.setColumn(i, aggregationFunctions[i - numKeyColumns].serializeIntermediateResult(value));
            }
          } else {
            if (value == null) {
              value = nullPlaceholders[i];
              nullBitmaps[i].add(rowId);
            }
            assert value != null;
            setDataTableColumn(storedColumnDataTypes[i], dataTableBuilder, i, value);
          }
        }
        dataTableBuilder.finishRow();
        numRowsAdded++;
        rowId++;
      }
      for (RoaringBitmap nullBitmap : nullBitmaps) {
        dataTableBuilder.setNullRowIds(nullBitmap);
      }
    } else {
      while (iterator.hasNext()) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
        dataTableBuilder.startRow();
        Object[] values = iterator.next().getValues();
        for (int i = 0; i < numColumns; i++) {
          Object value = values[i];
          if (value == null) {
            dataTableBuilder.setNull(i);
          } else if (storedColumnDataTypes[i] == ColumnDataType.OBJECT) {
            dataTableBuilder.setColumn(i, aggregationFunctions[i - numKeyColumns].serializeIntermediateResult(value));
          } else {
            setDataTableColumn(storedColumnDataTypes[i], dataTableBuilder, i, value);
          }
        }
        dataTableBuilder.finishRow();
        numRowsAdded++;
      }
    }
    return dataTableBuilder.build();
  }

  private void setDataTableColumn(ColumnDataType storedColumnDataType, DataTableBuilder dataTableBuilder,
      int columnIndex, Object value)
      throws IOException {
    switch (storedColumnDataType) {
      case INT:
        dataTableBuilder.setColumn(columnIndex, (int) value);
        break;
      case LONG:
        dataTableBuilder.setColumn(columnIndex, (long) value);
        break;
      case FLOAT:
        dataTableBuilder.setColumn(columnIndex, (float) value);
        break;
      case DOUBLE:
        dataTableBuilder.setColumn(columnIndex, (double) value);
        break;
      case BIG_DECIMAL:
        dataTableBuilder.setColumn(columnIndex, (BigDecimal) value);
        break;
      case STRING:
        dataTableBuilder.setColumn(columnIndex, value.toString());
        break;
      case BYTES:
        dataTableBuilder.setColumn(columnIndex, (ByteArray) value);
        break;
      case INT_ARRAY:
        if (value instanceof IntArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toIntArray((IntArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (int[]) value);
        }
        break;
      case LONG_ARRAY:
        if (value instanceof LongArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toLongArray((LongArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (long[]) value);
        }
        break;
      case FLOAT_ARRAY:
        if (value instanceof FloatArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toFloatArray((FloatArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (float[]) value);
        }
        break;
      case DOUBLE_ARRAY:
        if (value instanceof DoubleArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toDoubleArray((DoubleArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (double[]) value);
        }
        break;
      case STRING_ARRAY:
        if (value instanceof ObjectArrayList) {
          //noinspection unchecked
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toStringArray((ObjectArrayList<String>) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (String[]) value);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedColumnDataType);
    }
  }

  @Override
  public Map<String, String> getResultsMetadata() {
    Map<String, String> metadata = super.getResultsMetadata();
    if (_groupsTrimmed) {
      metadata.put(MetadataKey.GROUPS_TRIMMED.getName(), "true");
    }
    if (_numGroupsLimitReached) {
      metadata.put(MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), "true");
    }
    if (_numGroupsWarningLimitReached) {
      metadata.put(MetadataKey.NUM_GROUPS_WARNING_LIMIT_REACHED.getName(), "true");
    }
    metadata.put(MetadataKey.NUM_RESIZES.getName(), Integer.toString(_numResizes));
    metadata.put(MetadataKey.RESIZE_TIME_MS.getName(), Long.toString(_resizeTimeMs));
    return metadata;
  }
}
