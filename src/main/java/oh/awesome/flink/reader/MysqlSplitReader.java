package oh.awesome.flink.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.converter.MySQLRowConverter;
import oh.awesome.flink.split.MySqlSplit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MysqlSplitReader implements SplitReader<RowData, MySqlSplit> {
    private final Connection connection;
    private final MySQLRowConverter converter;
    private final List<MySqlSplit> pendingReadSplits;
    private final Set<String> finishedSplits;

    public MysqlSplitReader(Connection connection, MySQLRowConverter converter) {
        this.connection = connection;
        this.converter = converter;
        this.pendingReadSplits = new ArrayList<>();
        this.finishedSplits = new HashSet<>();
    }

    // The heavier read and conversion logic is implemented here
    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        Map<String, Iterator<RowData>> splitRecords = pendingReadSplits.stream()
                .map(split -> {
                    List<RowData> result = new LinkedList<>();
                    try {
                        Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(split.getSelectSql());
                        while (resultSet.next()) {
                            result.add(converter.toInternal(resultSet));
                        }
                        resultSet.close();
                        statement.close();
                    } catch (SQLException throwable) {
                        throwable.printStackTrace();
                    }
                    return new Tuple2<>(split.splitId(), result.iterator());
                })
        .collect(HashMap::new,
                (map, tuple2) -> map.put(tuple2.f0, tuple2.f1),
                Map::putAll);

        Set<String> newFinishedSplits = pendingReadSplits.stream()
                .map(MySqlSplit::splitId)
                .collect(Collectors.toSet());
        finishedSplits.addAll(newFinishedSplits);

        pendingReadSplits.clear();

        return new MysqlRecordsWithSplitIds(splitRecords, finishedSplits);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
        pendingReadSplits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
    }
}
