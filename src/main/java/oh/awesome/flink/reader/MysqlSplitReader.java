package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.split.MySqlSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MysqlSplitReader implements SplitReader<RowData, MySqlSplit> {
    private List<MySqlSplit> unreadSplits;
    private List<MySqlSplit> readSplits;

    public MysqlSplitReader() {
        this.unreadSplits = new ArrayList<>();
        this.readSplits = new ArrayList<>();
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
        unreadSplits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
