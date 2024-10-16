package org.apache.pinot.segment.spi.index.creator;

import com.google.common.base.Preconditions;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public interface PrefixIndexCreator extends DictionaryBasedInvertedIndexCreator {

  /**
   * Seals the index and flushes it to disk.
   */
  @Override
  default void seal()
      throws IOException {
  }

  /**
   * Adds the given single value cell to the index.
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dictId An optional dictionary value of the cell. If there is no dictionary, -1 is received
   * @param stringLength maxlength of the prefix
   */
  default void add(@Nonnull Object value, int dictId, int stringLength)
      throws IOException {
    Preconditions.checkArgument(dictId >= 0, "A dictionary id is required");


  }

  /**
   * Adds the given multi value cell to the index
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param values The nonnull value of the cell. In case the cell was actually null, an empty array is received instead
   * @param dictIds An optional array of dictionary values. If there is no dictionary, null is received.
   * @param stringLength maxlength of the prefix
   */
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds, int stringLength)
      throws IOException {
    Preconditions.checkArgument(dictIds != null, "A dictionary id is required");

  }
}
