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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.BaseJsonConfig;

public class UpsertConfig extends BaseJsonConfig {

  public enum Mode {
    FULL, PARTIAL, NONE
  }

  public enum Strategy {
    OVERWRITE, IGNORE, INCREMENT, APPEND
  }

  public class PartialUpsertStrategy {
    private String _field;
    private Strategy _strategy;

    @JsonCreator
    public PartialUpsertStrategy(@JsonProperty(value = "field", required = true) String field,
        @JsonProperty(value = "strategy", required = true) Strategy strategy) {
      _field = field;
      _strategy = strategy;
    }
  }

  private final Mode _mode;

  private final Strategy _globalStrategy;

  private final List<PartialUpsertStrategy> _partialUpsertStrategies;

  @JsonCreator
  public UpsertConfig(@JsonProperty(value = "mode", required = true) Mode mode,
      @JsonProperty(value = "globalStrategy") Strategy globalStrategy,
      @JsonProperty(value = "partialUpsertStrategies") List<PartialUpsertStrategy> partialUpsertStrategies) {
    Preconditions.checkArgument(mode != null, "Upsert mode must be configured");
    _mode = mode;
    _globalStrategy = globalStrategy != null ? globalStrategy : Strategy.OVERWRITE;
    _partialUpsertStrategies = partialUpsertStrategies != null ? partialUpsertStrategies : new ArrayList<PartialUpsertStrategy>();
  }

  public Mode getMode() {
    return _mode;
  }

  public Strategy getGlobalStrategy() {
    return _globalStrategy;
  }

  public List<PartialUpsertStrategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }
}
