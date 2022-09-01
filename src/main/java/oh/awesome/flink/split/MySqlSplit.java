package oh.awesome.flink.split;

import org.apache.flink.api.connector.source.SourceSplit;

import com.google.common.base.Objects;

public class MySqlSplit implements SourceSplit {

    private final ColumnMeta columnMeta;
    private final Range range;

    public MySqlSplit(ColumnMeta columnMeta, Range range) {
        this.columnMeta = columnMeta;
        this.range = range;
    }

    public ColumnMeta getColumnMeta() {
        return columnMeta;
    }

    public Range getRange() {
        return range;
    }

    @Override
    public String splitId() {
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MySqlSplit that = (MySqlSplit) o;
        return Objects.equal(columnMeta, that.columnMeta) && Objects.equal(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnMeta, range);
    }

    @Override
    public String toString() {
        return String.join(":", columnMeta.toString(), range.toString());
    }
}
