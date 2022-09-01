package oh.awesome.flink.dialect;

public class MySQLDialect {
    public static String getMaxValueStatement(String schema, String table, String column) {
        return getBestValueStatement("MAX", schema, table, column);
    }

    public static String getMinValueStatement(String schema, String table, String column) {
        return getBestValueStatement("MIN", schema, table, column);
    }

    private static String getBestValueStatement(String function, String schema, String table, String column) {
        final String sql = "SELECT %s(%s) FROM %s.%s";
        return String.format(sql,
                function,
                quoteIdentifier(column),
                quoteIdentifier(schema),
                quoteIdentifier(table));
    }

    public static String quoteIdentifier(String anyKeyword) {
        return String.join("", "`", anyKeyword, "`");
    }
}