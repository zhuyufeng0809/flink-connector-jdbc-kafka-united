package oh.awesome.flink.dialect;

import oh.awesome.flink.split.Range;

import java.util.Arrays;
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

    public static String getSelectFromStatement(String schema,
                                                String table,
                                                String[] selectFields,
                                                String splitColumn,
                                                Range range) {
        final String sql = "SELECT %s FROM %s.%s WHERE %s BETWEEN %s AND %s";
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(MySQLDialect::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        return String.format(sql,
                selectExpressions,
                quoteIdentifier(schema),
                quoteIdentifier(table),
                quoteIdentifier(splitColumn),
                range.getLowerBound().toString(),
                range.getUpperBound().toString());
    }

    private static String quoteIdentifier(String anyKeyword) {
        if (anyKeyword.equals("*")) {
            return anyKeyword;
        } else {
            return String.join("", "`", anyKeyword, "`");
        }
    }
}