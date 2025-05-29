package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.RoaringBitmap;

public class PrefixMatchTransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "prefixMatch";
    private TransformFunction _columnTransformFunction;
    private String _prefix;
    private InvertedIndexReader _prefixIndexReader;

    @Override
    public String getName() {
        return FUNCTION_NAME;
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
        // Check arguments
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("Exactly 2 arguments are required for transform function: " + FUNCTION_NAME);
        }

        // Get column name
        TransformFunction firstArgument = arguments.get(0);
        if (!(firstArgument instanceof IdentifierTransformFunction)) {
            throw new IllegalArgumentException("First argument must be a column name");
        }
        String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
        _columnTransformFunction = firstArgument;

        // Get prefix
        TransformFunction secondArgument = arguments.get(1);
        if (!(secondArgument instanceof LiteralTransformFunction)) {
            throw new IllegalArgumentException("Second argument must be a string literal");
        }
        _prefix = ((LiteralTransformFunction) secondArgument).getStringLiteral();

        // Get prefix index reader
        DataSource dataSource = columnContextMap.get(columnName).getDataSource();
        if (dataSource == null) {
            throw new IllegalArgumentException("Cannot apply " + FUNCTION_NAME + " on column: " + columnName + " without prefix index");
        }
        _prefixIndexReader = dataSource.getPrefixIndex();
        if (_prefixIndexReader == null) {
            throw new IllegalArgumentException("Cannot apply " + FUNCTION_NAME + " on column: " + columnName + " without prefix index");
        }
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
        return BOOLEAN_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
        int length = valueBlock.getNumDocs();
        initIntValuesSV(length);

        // Get document IDs from prefix index reader
        RoaringBitmap matchingDocIds = _prefixIndexReader.getDocIds(_prefix);
        
        // For each document in the block
        int[] docIds = valueBlock.getDocIds();
        for (int i = 0; i < length; i++) {
            // Return 1 if document matches prefix, 0 otherwise
            _intValuesSV[i] = matchingDocIds.contains(docIds[i]) ? 1 : 0;
        }

        return _intValuesSV;
    }
} 