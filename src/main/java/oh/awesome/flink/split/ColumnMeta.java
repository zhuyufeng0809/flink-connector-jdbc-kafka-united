package oh.awesome.flink.split;

import java.io.Serializable;

public class ColumnMeta implements Serializable {
    private final String schemaName;
    private final String tableName;
    private final String columnName;

    public ColumnMeta(String schemaName, String tableName, String columnName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
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
        return com.google.common.base.Objects.equal(schemaName, that.schemaName) && com.google.common.base.Objects.equal(tableName, that.tableName) && com.google.common.base.Objects.equal(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(schemaName, tableName, columnName);
    }

    @Override
    public String toString() {
        return String.join(".", schemaName, tableName, columnName);
    }
}
