package oh.awesome.flink.split;

import org.apache.flink.api.connector.source.SourceSplit;

public class MySqlSplit implements SourceSplit {

    private final ColumnMeta columnMeta;
    private final Long lowerBound;
    private final Long upperBound;

    public MySqlSplit(ColumnMeta columnMeta, Long lowerBound, Long upperBound) {
        this.columnMeta = columnMeta;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public String splitId() {
        return null;
    }

    @Override
    public String toString() {
        String range = String.join("-", lowerBound.toString(), upperBound.toString());
        return String.join(":", columnMeta.toString(), range);
    }
}
