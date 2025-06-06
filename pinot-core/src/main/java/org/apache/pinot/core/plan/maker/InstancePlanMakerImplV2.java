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
package org.apache.pinot.core.plan.maker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.plan.AcquireReleaseColumnsSegmentPlanNode;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.DistinctPlanNode;
import org.apache.pinot.core.plan.GlobalPlanImplV0;
import org.apache.pinot.core.plan.GroupByPlanNode;
import org.apache.pinot.core.plan.InstanceResponsePlanNode;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.plan.StreamingInstanceResponsePlanNode;
import org.apache.pinot.core.plan.StreamingSelectionPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>InstancePlanMakerImplV2</code> class is the default implementation of {@link PlanMaker}.
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  public static final int DEFAULT_NUM_THREADS_EXTRACT_FINAL_RESULT = 1;
  public static final int DEFAULT_CHUNK_SIZE_EXTRACT_FINAL_RESULT = 10_000;

  // The following fields are deprecated and will be removed after 1.4 release
  // Use CommonConstants.Server.* instead
  @Deprecated
  public static final String MAX_EXECUTION_THREADS_KEY = Server.MAX_EXECUTION_THREADS;
  @Deprecated
  public static final int DEFAULT_MAX_EXECUTION_THREADS = Server.DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS;
  @Deprecated
  public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY = Server.MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  @Deprecated
  public static final int DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
      Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  @Deprecated
  public static final String MIN_INITIAL_INDEXED_TABLE_CAPACITY_KEY = Server.MIN_INITIAL_INDEXED_TABLE_CAPACITY;
  @Deprecated
  public static final int DEFAULT_MIN_INITIAL_INDEXED_TABLE_CAPACITY =
      Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY;
  @Deprecated
  public static final String NUM_GROUPS_LIMIT_KEY = Server.NUM_GROUPS_LIMIT;
  @Deprecated
  public static final int DEFAULT_NUM_GROUPS_LIMIT = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT;
  @Deprecated
  public static final String MIN_SEGMENT_GROUP_TRIM_SIZE_KEY = Server.MIN_SEGMENT_GROUP_TRIM_SIZE;
  @Deprecated
  public static final int DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE =
      Server.DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE;
  @Deprecated
  public static final String MIN_SERVER_GROUP_TRIM_SIZE_KEY = Server.MIN_SERVER_GROUP_TRIM_SIZE;
  @Deprecated
  public static final int DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE = Server.DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE;
  @Deprecated
  public static final String GROUPBY_TRIM_THRESHOLD_KEY = Server.GROUPBY_TRIM_THRESHOLD;
  @Deprecated
  public static final int DEFAULT_GROUPBY_TRIM_THRESHOLD = Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);

  private final FetchPlanner _fetchPlanner = FetchPlannerRegistry.getPlanner();
  private int _maxExecutionThreads = Server.DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS;
  private int _maxInitialResultHolderCapacity = Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  private int _minInitialIndexedTableCapacity = Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY;
  // Limit on number of groups stored for each segment, beyond which no new group will be created
  private int _numGroupsLimit = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT;
  // Warning limit on number of groups stored for each segment
  private int _numGroupsWarningLimit = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT;
  // Used for SQL GROUP BY (server combine)
  private int _minSegmentGroupTrimSize = Server.DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE;
  private int _minServerGroupTrimSize = Server.DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE;
  private int _groupByTrimThreshold = Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD;

  @Override
  public void init(PinotConfiguration queryExecutorConfig) {
    _maxExecutionThreads = queryExecutorConfig.getProperty(Server.MAX_EXECUTION_THREADS,
        Server.DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS);
    _maxInitialResultHolderCapacity = queryExecutorConfig.getProperty(Server.MAX_INITIAL_RESULT_HOLDER_CAPACITY,
        Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _minInitialIndexedTableCapacity = queryExecutorConfig.getProperty(Server.MIN_INITIAL_INDEXED_TABLE_CAPACITY,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY);
    _numGroupsLimit =
        queryExecutorConfig.getProperty(Server.NUM_GROUPS_LIMIT, Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT);
    Preconditions.checkState(_maxInitialResultHolderCapacity <= _numGroupsLimit,
        "Invalid configuration: maxInitialResultHolderCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _maxInitialResultHolderCapacity, _numGroupsLimit);
    Preconditions.checkState(_minInitialIndexedTableCapacity <= _numGroupsLimit,
        "Invalid configuration: minInitialIndexedTableCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _minInitialIndexedTableCapacity, _numGroupsLimit);
    _numGroupsWarningLimit = queryExecutorConfig.getProperty(Server.NUM_GROUPS_WARN_LIMIT,
        Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT);
    _minSegmentGroupTrimSize = queryExecutorConfig.getProperty(Server.MIN_SEGMENT_GROUP_TRIM_SIZE,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE);
    _minServerGroupTrimSize = queryExecutorConfig.getProperty(Server.MIN_SERVER_GROUP_TRIM_SIZE,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE);
    _groupByTrimThreshold = queryExecutorConfig.getProperty(Server.GROUPBY_TRIM_THRESHOLD,
        Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD);
    Preconditions.checkState(_groupByTrimThreshold > 0,
        "Invalid configurable: groupByTrimThreshold: %d must be positive", _groupByTrimThreshold);
    LOGGER.info("Initialized plan maker with maxExecutionThreads: {}, maxInitialResultHolderCapacity: {}, "
            + "numGroupsLimit: {}, minSegmentGroupTrimSize: {}, minServerGroupTrimSize: {}, groupByTrimThreshold: {}",
        _maxExecutionThreads, _maxInitialResultHolderCapacity, _numGroupsLimit, _minSegmentGroupTrimSize,
        _minServerGroupTrimSize, _groupByTrimThreshold);
  }

  @VisibleForTesting
  public void setMaxInitialResultHolderCapacity(int maxInitialResultHolderCapacity) {
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
  }

  @VisibleForTesting
  public void setMinInitialIndexedTableCapacity(int minInitialIndexedTableCapacity) {
    _minInitialIndexedTableCapacity = minInitialIndexedTableCapacity;
  }

  @VisibleForTesting
  public void setNumGroupsLimit(int numGroupsLimit) {
    _numGroupsLimit = numGroupsLimit;
  }

  @VisibleForTesting
  public void setMinSegmentGroupTrimSize(int minSegmentGroupTrimSize) {
    _minSegmentGroupTrimSize = minSegmentGroupTrimSize;
  }

  @VisibleForTesting
  public void setMinServerGroupTrimSize(int minServerGroupTrimSize) {
    _minServerGroupTrimSize = minServerGroupTrimSize;
  }

  @VisibleForTesting
  public void setGroupByTrimThreshold(int groupByTrimThreshold) {
    _groupByTrimThreshold = groupByTrimThreshold;
  }

  public Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
      ExecutorService executorService, ServerMetrics serverMetrics) {
    applyQueryOptions(queryContext);

    int numSegments = segmentContexts.size();
    List<PlanNode> planNodes = new ArrayList<>(numSegments);
    List<FetchContext> fetchContexts;
    if (queryContext.isEnablePrefetch()) {
      fetchContexts = new ArrayList<>(numSegments);
      for (SegmentContext segmentContext : segmentContexts) {
        FetchContext fetchContext =
            _fetchPlanner.planFetchForProcessing(segmentContext.getIndexSegment(), queryContext);
        fetchContexts.add(fetchContext);
        planNodes.add(
            new AcquireReleaseColumnsSegmentPlanNode(makeSegmentPlanNode(segmentContext, queryContext), segmentContext,
                fetchContext));
      }
    } else {
      fetchContexts = Collections.emptyList();
      for (SegmentContext segmentContext : segmentContexts) {
        planNodes.add(makeSegmentPlanNode(segmentContext, queryContext));
      }
    }

    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, executorService, null);
    return new GlobalPlanImplV0(
        new InstanceResponsePlanNode(combinePlanNode, segmentContexts, fetchContexts, queryContext));
  }

  private void applyQueryOptions(QueryContext queryContext) {
    Map<String, String> queryOptions = queryContext.getQueryOptions();

    // Set skipUpsert
    queryContext.setSkipUpsert(QueryOptionsUtils.isSkipUpsert(queryOptions));

    // Set skipStarTree
    queryContext.setSkipStarTree(QueryOptionsUtils.isSkipStarTree(queryOptions));

    // Set accurateGroupByWithoutOrderBy
    queryContext.setAccurateGroupByWithoutOrderBy(
        QueryOptionsUtils.isAccurateGroupByWithoutOrderBy(queryOptions));

    // Set skipScanFilterReorder
    queryContext.setSkipScanFilterReorder(QueryOptionsUtils.isSkipScanFilterReorder(queryOptions));

    queryContext.setSkipIndexes(QueryOptionsUtils.getSkipIndexes(queryOptions));

    // Set maxExecutionThreads
    int maxExecutionThreads;
    Integer maxExecutionThreadsFromQuery = QueryOptionsUtils.getMaxExecutionThreads(queryOptions);
    if (maxExecutionThreadsFromQuery != null) {
      // Do not allow query to override the execution threads over the instance-level limit
      if (_maxExecutionThreads > 0) {
        maxExecutionThreads = Math.min(_maxExecutionThreads, maxExecutionThreadsFromQuery);
      } else {
        maxExecutionThreads = maxExecutionThreadsFromQuery;
      }
    } else {
      maxExecutionThreads = _maxExecutionThreads;
    }
    queryContext.setMaxExecutionThreads(maxExecutionThreads);

    // Set group-by query options
    if (QueryContextUtils.isAggregationQuery(queryContext) && queryContext.getGroupByExpressions() != null) {
      // Set maxInitialResultHolderCapacity
      Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(queryOptions);
      if (maxInitialResultHolderCapacity != null) {
        queryContext.setMaxInitialResultHolderCapacity(maxInitialResultHolderCapacity);
      } else {
        queryContext.setMaxInitialResultHolderCapacity(_maxInitialResultHolderCapacity);
      }
      // Set initialResultTableCapacity
      Integer minInitialIndexedTableCapacity = QueryOptionsUtils.getMinInitialIndexedTableCapacity(queryOptions);
      if (minInitialIndexedTableCapacity != null) {
        queryContext.setMinInitialIndexedTableCapacity(minInitialIndexedTableCapacity);
      } else {
        queryContext.setMinInitialIndexedTableCapacity(_minInitialIndexedTableCapacity);
      }
      // Set numGroupsLimit
      Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(queryOptions);
      if (numGroupsLimit != null) {
        queryContext.setNumGroupsLimit(numGroupsLimit);
      } else {
        queryContext.setNumGroupsLimit(_numGroupsLimit);
      }
      // Set numGroupsWarningThreshold
      queryContext.setNumGroupsWarningLimit(_numGroupsWarningLimit);
      // Set minSegmentGroupTrimSize
      Integer minSegmentGroupTrimSizeFromQuery = QueryOptionsUtils.getMinSegmentGroupTrimSize(queryOptions);
      if (minSegmentGroupTrimSizeFromQuery != null) {
        queryContext.setMinSegmentGroupTrimSize(minSegmentGroupTrimSizeFromQuery);
      } else {
        queryContext.setMinSegmentGroupTrimSize(_minSegmentGroupTrimSize);
      }
      // Set minServerGroupTrimSize
      Integer minServerGroupTrimSizeFromQuery = QueryOptionsUtils.getMinServerGroupTrimSize(queryOptions);
      int minServerGroupTrimSize =
          minServerGroupTrimSizeFromQuery != null ? minServerGroupTrimSizeFromQuery : _minServerGroupTrimSize;
      queryContext.setMinServerGroupTrimSize(minServerGroupTrimSize);
      // Set groupTrimThreshold
      Integer groupTrimThreshold = QueryOptionsUtils.getGroupTrimThreshold(queryOptions);
      if (groupTrimThreshold != null) {
        queryContext.setGroupTrimThreshold(groupTrimThreshold);
      } else {
        queryContext.setGroupTrimThreshold(_groupByTrimThreshold);
      }
      // Set numThreadsExtractFinalResult
      Integer numThreadsExtractFinalResult = QueryOptionsUtils.getNumThreadsExtractFinalResult(queryOptions);
      if (numThreadsExtractFinalResult != null) {
        queryContext.setNumThreadsExtractFinalResult(numThreadsExtractFinalResult);
      } else {
        queryContext.setNumThreadsExtractFinalResult(DEFAULT_NUM_THREADS_EXTRACT_FINAL_RESULT);
      }
      // Set chunkSizeExtractFinalResult
      Integer chunkSizeExtractFinalResult = QueryOptionsUtils.getChunkSizeExtractFinalResult(queryOptions);
      if (chunkSizeExtractFinalResult != null) {
        queryContext.setChunkSizeExtractFinalResult(chunkSizeExtractFinalResult);
      } else {
        queryContext.setChunkSizeExtractFinalResult(DEFAULT_CHUNK_SIZE_EXTRACT_FINAL_RESULT);
      }
    }
  }

  @Override
  public PlanNode makeSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    rewriteQueryContextWithHints(queryContext, segmentContext.getIndexSegment());
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      if (groupByExpressions != null) {
        // Group-by query
        return new GroupByPlanNode(segmentContext, queryContext);
      } else {
        // Aggregation query
        return new AggregationPlanNode(segmentContext, queryContext);
      }
    } else if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return new SelectionPlanNode(segmentContext, queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return new DistinctPlanNode(segmentContext, queryContext);
    }
  }

  public Plan makeStreamingInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
      ExecutorService executorService, ResultsBlockStreamer streamer, ServerMetrics serverMetrics) {
    applyQueryOptions(queryContext);

    int numSegments = segmentContexts.size();
    List<PlanNode> planNodes = new ArrayList<>(numSegments);
    List<FetchContext> fetchContexts;
    if (queryContext.isEnablePrefetch()) {
      fetchContexts = new ArrayList<>(numSegments);
      for (SegmentContext segmentContext : segmentContexts) {
        FetchContext fetchContext =
            _fetchPlanner.planFetchForProcessing(segmentContext.getIndexSegment(), queryContext);
        fetchContexts.add(fetchContext);
        planNodes.add(
            new AcquireReleaseColumnsSegmentPlanNode(makeStreamingSegmentPlanNode(segmentContext, queryContext),
                segmentContext, fetchContext));
      }
    } else {
      fetchContexts = Collections.emptyList();
      for (SegmentContext segmentContext : segmentContexts) {
        planNodes.add(makeStreamingSegmentPlanNode(segmentContext, queryContext));
      }
    }

    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, executorService, streamer);
    return new GlobalPlanImplV0(
        new StreamingInstanceResponsePlanNode(combinePlanNode, segmentContexts, fetchContexts, queryContext, streamer));
  }

  @Override
  public PlanNode makeStreamingSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    if (QueryContextUtils.isSelectionOnlyQuery(queryContext) && queryContext.getLimit() != 0) {
      // Use streaming operator only for non-empty selection-only query
      rewriteQueryContextWithHints(queryContext, segmentContext.getIndexSegment());
      return new StreamingSelectionPlanNode(segmentContext, queryContext);
    } else {
      return makeSegmentPlanNode(segmentContext, queryContext);
    }
  }

  /**
   * In-place rewrite QueryContext based on the information from local IndexSegment.
   *
   * @param queryContext
   * @param indexSegment
   */
  @VisibleForTesting
  public static void rewriteQueryContextWithHints(QueryContext queryContext, IndexSegment indexSegment) {
    Map<ExpressionContext, ExpressionContext> expressionOverrideHints = queryContext.getExpressionOverrideHints();
    if (MapUtils.isEmpty(expressionOverrideHints)) {
      return;
    }

    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    selectExpressions.replaceAll(
        expression -> overrideWithExpressionHints(expression, indexSegment, expressionOverrideHints));

    List<Pair<AggregationFunction, FilterContext>> filtAggrFuns = queryContext.getFilteredAggregationFunctions();
    if (filtAggrFuns != null) {
      for (Pair<AggregationFunction, FilterContext> filteredAggregationFunction : filtAggrFuns) {
        FilterContext right = filteredAggregationFunction.getRight();
        if (right != null) {
          overrideWithExpressionHints(right, indexSegment, expressionOverrideHints);
        }
      }
    }

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (CollectionUtils.isNotEmpty(groupByExpressions)) {
      groupByExpressions.replaceAll(
          expression -> overrideWithExpressionHints(expression, indexSegment, expressionOverrideHints));
    }

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (CollectionUtils.isNotEmpty(orderByExpressions)) {
      orderByExpressions.replaceAll(expression -> new OrderByExpressionContext(
          overrideWithExpressionHints(expression.getExpression(), indexSegment, expressionOverrideHints),
          expression.isAsc()));
    }

    // In-place override
    FilterContext filter = queryContext.getFilter();
    if (filter != null) {
      overrideWithExpressionHints(filter, indexSegment, expressionOverrideHints);
    }

    // In-place override
    FilterContext havingFilter = queryContext.getHavingFilter();
    if (havingFilter != null) {
      overrideWithExpressionHints(havingFilter, indexSegment, expressionOverrideHints);
    }
  }

  @VisibleForTesting
  public static void overrideWithExpressionHints(FilterContext filter, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    if (filter.getChildren() != null) {
      // AND, OR, NOT
      for (FilterContext child : filter.getChildren()) {
        overrideWithExpressionHints(child, indexSegment, expressionOverrideHints);
      }
    } else {
      // PREDICATE
      Predicate predicate = filter.getPredicate();
      predicate.setLhs(overrideWithExpressionHints(predicate.getLhs(), indexSegment, expressionOverrideHints));
    }
  }

  @VisibleForTesting
  public static ExpressionContext overrideWithExpressionHints(ExpressionContext expression, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }
    ExpressionContext overrideExpression = expressionOverrideHints.get(expression);
    if (overrideExpression != null && overrideExpression.getIdentifier() != null && indexSegment.getColumnNames()
        .contains(overrideExpression.getIdentifier())) {
      return overrideExpression;
    }
    expression.getFunction()
        .getArguments()
        .replaceAll(argument -> overrideWithExpressionHints(argument, indexSegment, expressionOverrideHints));
    return expression;
  }
}
