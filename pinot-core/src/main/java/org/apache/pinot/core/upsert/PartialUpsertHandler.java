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
package org.apache.pinot.core.upsert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class PartialUpsertHandler {
  private List<PartialUpsertMerger> _mergers;

  /**
   * Initializes the partial upsert merger with upsert config. Different fields can have different merge strategies.
   *
   * @param globalUpsertStrategy can be derived into fields to upsert strategies map.
   * @param partialUpsertStrategy can be derived into fields to upsert strategies map.
   */
  public void init(UpsertConfig.STRATEGY globalUpsertStrategy, Map<String, UpsertConfig.STRATEGY> partialUpsertStrategy, Map<String, String> customMergeStrategy) {
    _mergers = new ArrayList<>();

    for (Map.Entry<String, UpsertConfig.STRATEGY> entry : partialUpsertStrategy.entrySet()) {
      if (entry.getValue() == UpsertConfig.STRATEGY.IGNORE) {
        _mergers.add(new IgnoreMerger(entry.getKey()));
      } else if (entry.getValue() == UpsertConfig.STRATEGY.INCREMENT) {
        _mergers.add(new IncrementMerger(entry.getKey()));
      } else if (entry.getValue() == UpsertConfig.STRATEGY.OVERWRITE) {
        _mergers.add(new OverwriteMerger(entry.getKey()));
      }
    }

    for (Map.Entry<String,String> entry : customMergeStrategy.entrySet()) {
      _mergers.add(new CustomMerger(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * Handle partial upsert merge for given fieldName.
   *
   * @param previousRecord the last derived full record during ingestion.
   * @param newRecord the new consumed record.
   * @return a new row after merge
   */
  public GenericRow merge(GenericRow previousRecord, GenericRow newRecord) {

    for (PartialUpsertMerger merger: _mergers) {
      newRecord = merger.merge(previousRecord, newRecord);
    }

    return newRecord;
  }
}
