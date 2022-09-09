package oh.awesome.flink.split;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

public class ColumnMeta implements Serializable {
    private final String schemaName;
    private final String tableName;
    private final String splitColumnName;
    private final List<String> fieldNames;

    public ColumnMeta(String schemaName, String tableName, String splitColumnName, List<String> fieldNames) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.splitColumnName = splitColumnName;
        this.fieldNames = fieldNames;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSplitColumnName() {
        return splitColumnName;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnMeta that = (ColumnMeta) o;
        return Objects.equal(schemaName, that.schemaName) && Objects.equal(tableName, that.tableName) && Objects.equal(splitColumnName, that.splitColumnName) && Objects.equal(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schemaName, tableName, splitColumnName, fieldNames);
    }
}
