package oh.awesome.flink.split;

import java.io.Serializable;

public class ColumnMeta implements Serializable {
    String schemaName;
    String tableName;
    String columnName;

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
    public String toString() {
        return String.join(".", schemaName, tableName, columnName);
    }
}
