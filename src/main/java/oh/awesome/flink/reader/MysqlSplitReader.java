package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.split.MySqlSplit;

import java.io.IOException;

public class MysqlSplitReader implements SplitReader<RowData, MySqlSplit> {

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
