package org.apache.pinot.segment.local.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;


public class DeltaOfDeltaCompressor implements ChunkCompressor {

  private DeltaOfDeltaCompressor() {}

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    return 0;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    return 0;
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.DELTA_OF_DELTA;
  }
}
