package org.apache.pinot.segment.local.segment.index.readers;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class PrefixIndexReaderImpl implements InvertedIndexReader {
    private final PinotDataBuffer _dataBuffer;
    private final long[] _bitmapOffsets;
    private final int _prefixLength;

    public PrefixIndexReaderImpl(String columnName, int prefixLength) throws IOException {
        File indexFile = new File(columnName + V1Constants.Indexes.PREFIX_INDEX_FILE_EXTENSION);
        _dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        _prefixLength = prefixLength;

        // Read the number of bitmaps and their offsets
        int numBitmaps = _dataBuffer.getInt(0);
        _bitmapOffsets = new long[numBitmaps];
        long offset = 4;
        for (int i = 0; i < numBitmaps; i++) {
            _bitmapOffsets[i] = offset;
            offset += _dataBuffer.getInt(offset) + 4;
        }
    }

    @Override
    public RoaringBitmap getDocIds(String prefix) {
        // Find the bitmap for the given prefix
        for (int i = 0; i < _bitmapOffsets.length; i++) {
            long offset = _bitmapOffsets[i];
            int length = _dataBuffer.getInt(offset);
            byte[] bytes = new byte[length];
            _dataBuffer.copyTo(offset + 4, bytes);
            ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(bytes);
            
            // Check if this bitmap contains documents matching the prefix
            if (bitmap.getCardinality() > 0) {
                return bitmap.toRoaringBitmap();
            }
        }
        return new RoaringBitmap();
    }

    @Override
    public void close() throws IOException {
        _dataBuffer.close();
    }
} 