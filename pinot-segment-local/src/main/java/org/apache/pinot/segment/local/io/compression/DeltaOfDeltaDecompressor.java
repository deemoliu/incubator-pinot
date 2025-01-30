package org.apache.pinot.segment.local.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;


public class DeltaOfDeltaDecompressor implements ChunkDecompressor {

  private DeltaOfDeltaDecompressor() {}

  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    return 0;
  }

  public int decompressedLength(ByteBuffer compressedInput) {
    return -1;
  }
}
