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
package org.apache.pinot.segment.local.upsert.validdocs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ValidDocsSnapshotContainer implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final MutableRoaringBitmap _validDocsSnapshot;

  public ValidDocsSnapshotContainer(File segmentDirectory, SegmentMetadataImpl segmentMetadata)
      throws IOException {
    String validDocsFileName = segmentMetadata.getName() + V1Constants.Indexes.VALID_DOCS_FILE_EXTENSION;
    File validDocsFile = new File(segmentDirectory, validDocsFileName);

    _dataBuffer = PinotDataBuffer.mapFile(validDocsFile, true, 0, validDocsFile.length(), ByteOrder.LITTLE_ENDIAN,
        "Valid Doc Snapshot data buffer");

    if (!validDocsFile.exists()) {
      _validDocsSnapshot = new MutableRoaringBitmap();
    } else {
      _validDocsSnapshot = new ImmutableRoaringBitmap(_dataBuffer.toDirectByteBuffer(0, (int) _dataBuffer.size()))
          .toMutableRoaringBitmap();
    }
  }

  public ImmutableRoaringBitmap getValidDocsSnapshot() {
    return _validDocsSnapshot;
  }

  @Override
  public void close()
      throws IOException {
    _dataBuffer.close();
  }
}
