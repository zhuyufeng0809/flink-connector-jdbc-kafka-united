package oh.awesome.flink.dialect;

import oh.awesome.flink.split.Range;

import java.util.List;
import java.util.stream.Collectors;

public class MySQLDialect {
    public static String getSelectBestValueStatement(String schema,
                                                     String table,
                                                     String column) {
        String quotedColumn = quoteIdentifier(column);
        final String sql = "SELECT MIN(%s), MAX(%s) FROM %s.%s";
        return String.format(sql,
                quotedColumn,
                quotedColumn,
                quoteIdentifier(schema),
                quoteIdentifier(table));
    }

    public static String getSelectFromBetweenStatement(String schema,
                                                       String table,
                                                       List<String> selectFields,
                                                       String splitColumn,
                                                       Range range) {
        final String sql = " WHERE %s BETWEEN %s AND %s";
        String between = String.format(sql, quoteIdentifier(splitColumn), range.getLowerBound(), range.getUpperBound());
        return String.join("", getSelectFromStatement(schema, table, selectFields), between);
    }

    public static String getSelectFromStatement(String schema,
                                                       String table,
                                                       List<String> selectFields) {
        final String sql = "SELECT %s FROM %s.%s";
        String selectExpressions = selectFields.stream()
                        .map(MySQLDialect::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        return String.format(sql,
                selectExpressions,
                quoteIdentifier(schema),
                quoteIdentifier(table));
    }

    private static String quoteIdentifier(String anyKeyword) {
        if (anyKeyword.equals("*")) {
            return anyKeyword;
        } else {
            return String.join("", "`", anyKeyword, "`");
        }
    }
}