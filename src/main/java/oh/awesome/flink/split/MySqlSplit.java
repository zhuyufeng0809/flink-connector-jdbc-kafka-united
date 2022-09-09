package oh.awesome.flink.split;

import org.apache.flink.api.connector.source.SourceSplit;

import com.google.common.base.Objects;
import oh.awesome.flink.dialect.MySQLDialect;

public class MySqlSplit implements SourceSplit {

    private final ColumnMeta columnMeta;
    private final Range range;
    private final Integer id;

    public MySqlSplit(ColumnMeta columnMeta, Range range, Integer id) {
        this.columnMeta = columnMeta;
        this.range = range;
        this.id = id;
    }

    public Range getRange() {
        return range;
    }

    public Integer getId() {
        return id;
    }

    @Override
    public String splitId() {
        return getId().toString();
    }

    public String getSelectSql() {
        return MySQLDialect.getSelectFromBetweenStatement(
                columnMeta.getSchemaName(),
                columnMeta.getTableName(),
                columnMeta.getFieldNames(),
                columnMeta.getSplitColumnName(),
                getRange()
        );
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
        return Objects.equal(columnMeta, that.columnMeta) && Objects.equal(range, that.range) && Objects.equal(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnMeta, range, id);
    }

    @Override
    public String toString() {
        return String.join(":", columnMeta.toString(), range.toString());
    }
}
