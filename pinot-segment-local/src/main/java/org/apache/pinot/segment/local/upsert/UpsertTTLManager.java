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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertTTLManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertTTLManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 300L;  // Maximum of 5 minutes back-off to retry the deletion
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  private final ScheduledExecutorService _executorService;
  private Long _ttlTimeInSeconds;

  public UpsertTTLManager(Long ttlTime) {
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotUpsertTTLManagerExecutorService");
        return thread;
      }
    });
    // FIXME: input can be string with time unit, need to convert it
    // FIXME: INPUT SHOULDN'T UPDATE COMPARISON COLUMN BECAUSE WE NEED USE TIME COLUMN VALUE TO COMPARE.
    _ttlTimeInSeconds = ttlTime;
  }

  public void stop() {
    _executorService.shutdownNow();
  }

  protected void removeRecordWithDelay(Map<String, RecordLocation> upsertMetadata, long deletionDelaySeconds) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        removeRecordWithFullScan(upsertMetadata);
      }
    }, deletionDelaySeconds, TimeUnit.SECONDS);
  }

  protected synchronized void removeRecordWithFullScan(Map<String, RecordLocation> upsertMetadata) {
    for (String record : upsertMetadata.keySet()) {
      long eventTime = (long) upsertMetadata.get(record).getComparisonValue();
      if (eventTime < System.currentTimeMillis() - _ttlTimeInSeconds) {
        upsertMetadata.remove(record);
      }
    }
  }

  protected void removeRecordWithTTLInteval(Map<String, RecordLocation> pastUpsertMetadata,
      Map<String, RecordLocation> currentUpsertMetadata) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        removeRecordWithTwoMapsSwap(pastUpsertMetadata, currentUpsertMetadata);
      }
    }, _ttlTimeInSeconds, TimeUnit.SECONDS);
  }

  protected synchronized void removeRecordWithTwoMapsSwap(Map<String, RecordLocation> pastUpsertMetadata,
      Map<String, RecordLocation> currentUpsertMetadata) {
    pastUpsertMetadata.clear();
    pastUpsertMetadata.putAll(currentUpsertMetadata);
    currentUpsertMetadata.clear();
  }
}
