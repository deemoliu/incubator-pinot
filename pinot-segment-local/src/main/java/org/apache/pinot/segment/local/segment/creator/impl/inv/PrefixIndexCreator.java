package org.apache.pinot.segment.local.segment.creator.impl.inv;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class PrefixIndexCreator implements DictionaryBasedInvertedIndexCreator {
    private final File _indexFile;
    private final int _prefixLength;
    private final Map<String, RoaringBitmapWriter<RoaringBitmap>> _prefixToBitmapWriterMap;
    private int _nextDocId;

    public PrefixIndexCreator(String columnName, int prefixLength) {
        _indexFile = new File(columnName + V1Constants.Indexes.PREFIX_INDEX_FILE_EXTENSION);
        _prefixLength = prefixLength;
        _prefixToBitmapWriterMap = new HashMap<>();
        _nextDocId = 0;
    }

    @Override
    public void add(int dictId, Dictionary dictionary) {
        String value = dictionary.getStringValue(dictId);
        String prefix = value.substring(0, Math.min(_prefixLength, value.length()));
        _prefixToBitmapWriterMap.computeIfAbsent(prefix, k -> RoaringBitmapWriter.writer().runCompress(true).get())
            .add(_nextDocId);
        _nextDocId++;
    }

    @Override
    public void add(int[] dictIds, int length, Dictionary dictionary) {
        for (int i = 0; i < length; i++) {
            add(dictIds[i], dictionary);
        }
    }

    @Override
    public void seal() throws IOException {
        try (BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(_indexFile, _prefixToBitmapWriterMap.size())) {
            for (Map.Entry<String, RoaringBitmapWriter<RoaringBitmap>> entry : _prefixToBitmapWriterMap.entrySet()) {
                writer.add(entry.getValue().get());
            }
        }
    }

    @Override
    public void close() throws IOException {
        // No resources to clean up
    }
} 