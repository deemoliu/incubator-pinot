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
package org.apache.pinot.segment.local.segment.creator.impl.upsert;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ValidDocsSnapshotCreator implements Closeable {

  private final File _segmentDir;
  private File _validDocsSnapshotFile;
  private String _segmentName;
  private MutableRoaringBitmap _validDocsSnapshot = new MutableRoaringBitmap();

  public ValidDocsSnapshotCreator(File indexDir) {
    _segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
  }

  public void persist(MutableRoaringBitmap validDocsSnapshot, String segmentName)
      throws IOException {
    _segmentName = segmentName;
    _validDocsSnapshot = validDocsSnapshot;
    _validDocsSnapshotFile = new File(_segmentDir, _segmentName + V1Constants.Indexes.VALID_DOCS_FILE_EXTENSION);

    // Create valid doc snapshot file only if the bitmap is not empty
    if (!_validDocsSnapshot.isEmpty()) {
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_validDocsSnapshotFile))) {
        _validDocsSnapshot.serialize(outputStream);
      }
    }
  }

  @Override
  public void close() {
  }
}
