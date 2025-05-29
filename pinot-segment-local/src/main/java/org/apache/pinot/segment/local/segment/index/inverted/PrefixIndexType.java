package org.apache.pinot.segment.local.segment.index.inverted;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

public class PrefixIndexType implements IndexType<PrefixIndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator> {
    public static final String INDEX_DISPLAY_NAME = "prefix";
    public static final String INDEX_FILE_EXTENSION = ".prefix.idx";

    public static final PrefixIndexType INSTANCE = new PrefixIndexType();

    private PrefixIndexType() {
    }

    @Override
    public String getIndexName() {
        return INDEX_DISPLAY_NAME;
    }

    @Override
    public String getPrettyName() {
        return INDEX_DISPLAY_NAME;
    }

    @Override
    public String getFileExtension() {
        return INDEX_FILE_EXTENSION;
    }

    @Override
    public PrefixIndexConfig getDefaultConfig() {
        return PrefixIndexConfig.DEFAULT;
    }

    @Override
    public Class<PrefixIndexConfig> getIndexConfigClass() {
        return PrefixIndexConfig.class;
    }

    @Override
    public boolean isDefaultColumnIndex(Schema schema, FieldSpec fieldSpec) {
        return false;
    }

    @Override
    public PrefixIndexConfig getConfig(IndexingConfig indexingConfig) {
        return indexingConfig.getPrefixIndexConfig();
    }

    @Override
    public DictionaryBasedInvertedIndexCreator createIndexCreator(PrefixIndexConfig config, String columnName, 
            FieldSpec fieldSpec, int cardinality, int totalDocs, int maxLength) {
        return new PrefixIndexCreator(columnName, config.getPrefixLength());
    }

    @Override
    public InvertedIndexReader createIndexReader(PrefixIndexConfig config, String columnName, 
            FieldSpec fieldSpec, int cardinality, int totalDocs, int maxLength) {
        return new PrefixIndexReaderImpl(columnName, config.getPrefixLength());
    }

    public static class PrefixIndexConfig implements IndexConfig {
        public static final PrefixIndexConfig DEFAULT = new PrefixIndexConfig(3);

        private final int _prefixLength;

        @JsonCreator
        public PrefixIndexConfig(@JsonProperty("prefixLength") int prefixLength) {
            _prefixLength = prefixLength;
        }

        @JsonProperty("prefixLength")
        public int getPrefixLength() {
            return _prefixLength;
        }
    }
} 