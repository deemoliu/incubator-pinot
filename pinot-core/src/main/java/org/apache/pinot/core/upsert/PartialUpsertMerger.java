package org.apache.pinot.core.upsert;

import org.apache.pinot.spi.data.readers.GenericRow;


public interface PartialUpsertMerger {
  /**
   * Handle partial upsert merge for given fieldName.
   *
   * @param previousRecord the last derived full record during ingestion.
   * @param currentRecord the new consumed record.
   * @param fieldName merge records for the given field.
   * @return a new row after merge
   */
  GenericRow merge(GenericRow previousRecord, GenericRow currentRecord, String fieldName);
}