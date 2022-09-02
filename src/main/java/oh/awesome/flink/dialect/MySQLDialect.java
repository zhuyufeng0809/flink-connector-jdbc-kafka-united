package oh.awesome.flink.dialect;

public class MySQLDialect {
    public static String getBestValueStatement(String schema, String table, String column) {
        String quotedColumn = quoteIdentifier(column);
        final String sql = "SELECT MIN(%s), MAX(%s) FROM %s.%s";
        return String.format(sql,
                quotedColumn,
                quotedColumn,
                quoteIdentifier(schema),
                quoteIdentifier(table));
    }

    public static String quoteIdentifier(String anyKeyword) {
        return String.join("", "`", anyKeyword, "`");
    }
}