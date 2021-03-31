package org.apache.pinot.core.upsert;

import java.util.List;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;

//public class PartialUpsertHandler implements PartialUpsertMerger {
//  private final List<PartialUpsertMerger> _mergers;
//
//  /**
//   * Initializes the partial upsert merger with upsert config. Different fields can have different merge strategies.
//   *
//   * @param upsertConfig can be derived into fields to upsert strategies map.
//   * @throws Exception If an error occurs
//   */
//  public void init(UpsertConfig upsertConfig) throws Exception;
//
//  /**
//   * Handle partial upsert merge for given fieldName.
//   *
//   * @param previousRecord the last derived full record during ingestion.
//   * @param currentRecord the new consumed record.
//   * @param fieldName merge records for the given field.
//   * @return a new row after merge
//   */
//  public GenericRow merge(GenericRow previousRecord, GenericRow currentRecord, String fieldName);
//}