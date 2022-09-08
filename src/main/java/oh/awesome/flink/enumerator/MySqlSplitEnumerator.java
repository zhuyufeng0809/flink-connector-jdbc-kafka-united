package oh.awesome.flink.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.dialect.MySQLDialect;
import oh.awesome.flink.split.ColumnMeta;
import oh.awesome.flink.split.MySqlSplit;
import oh.awesome.flink.split.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class MySqlSplitEnumerator implements SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> {
    private final SplitEnumeratorContext<MySqlSplit> context;
    private final Properties config;
    private final Tuple2<Long, Long> bestValue;
    private Map<Integer, Set<MySqlSplit>> unassignedSplits;
    private final String[] fieldNames;

    public MySqlSplitEnumerator(SplitEnumeratorContext<MySqlSplit> context, Properties config, String[] fieldNames) throws Exception {
        this.context = context;
        this.config = config;
        this.bestValue = fetchSplitColumnBestValue();
        this.fieldNames = fieldNames;
    }

    @Override
    public void start() {
        List<MySqlSplit> allSplits = calculateAndDerivedSplits(bestValue.f0, bestValue.f1, Integer.parseInt(config.getProperty(ConfigOptions.SPLIT_NUM)));
        unassignedSplits = allSplits.stream()
                .collect(Collectors.groupingBy(
                        split -> split.getId() % context.currentParallelism(),
                        Collectors.toSet()
                ));
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        // todo
    }

    @Override
    public void addReader(int subtaskId) {
        Set<MySqlSplit> splits = unassignedSplits.remove(subtaskId);
        Map<Integer, List<MySqlSplit>> assignedSplits = new HashMap<>();
        assignedSplits.put(subtaskId, new ArrayList<>(splits));
        context.assignSplits(new SplitsAssignment<>(assignedSplits));
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public MysqlSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        // todo
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    private List<MySqlSplit> calculateAndDerivedSplits(long minValue, long maxValue, int splitsNum) {
        Preconditions.checkArgument(splitsNum > 0, "Split number must be positive");

        long maxElemCount = (maxValue - minValue) + 1;
        int actualSplitsNum = splitsNum > maxElemCount ? (int) maxElemCount : splitsNum;
        long splitSize = new Double(Math.ceil((double) maxElemCount / actualSplitsNum)).longValue();
        long bigBatchNum = maxElemCount - (splitSize - 1) * actualSplitsNum;

        long start = minValue;
        List<MySqlSplit> splits = new ArrayList<>();
        for (int i = 0; i < minValue; i++) {
            long end = start + splitSize - 1 - (i >= bigBatchNum ? 1 : 0);
            splits.add(new MySqlSplit(
                    new ColumnMeta(
                            config.getProperty(ConfigOptions.SCHEMA),
                            config.getProperty(ConfigOptions.TABLE),
                            config.getProperty(ConfigOptions.SPLIT_COLUMN),
                            fieldNames),
                    new Range(start, end), i));
            start = end + 1;
        }

        return splits;
    }

    private Tuple2<Long, Long> fetchSplitColumnBestValue() throws Exception {
        Connection connection = DriverManager.getConnection(
                config.getProperty(ConfigOptions.HOST),
                config.getProperty(ConfigOptions.USERNAME),
                config.getProperty(ConfigOptions.PASSWORD));

        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery(
                MySQLDialect.getSelectBestValueStatement(
                config.getProperty(ConfigOptions.SCHEMA),
                config.getProperty(ConfigOptions.TABLE),
                config.getProperty(ConfigOptions.SPLIT_COLUMN))
        );

        resultSet.next();
        Long lowerBound = resultSet.getLong(1);
        Long upperBound = resultSet.getLong(2);

        resultSet.close();
        statement.close();
        connection.close();

        return new Tuple2<>(lowerBound, upperBound);
    }
}
