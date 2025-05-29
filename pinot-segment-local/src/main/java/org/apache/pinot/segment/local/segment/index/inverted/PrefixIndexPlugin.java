package org.apache.pinot.segment.local.segment.index.inverted;

import org.apache.pinot.segment.spi.index.IndexPlugin;
import org.apache.pinot.segment.spi.index.IndexType;

public class PrefixIndexPlugin implements IndexPlugin {
    @Override
    public void init() {
        // Register the prefix index type
        IndexType.register(PrefixIndexType.INSTANCE);
    }
} 