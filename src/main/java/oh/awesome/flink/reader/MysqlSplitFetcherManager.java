package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.split.MySqlSplit;

import java.util.List;
import java.util.function.Supplier;

public class MysqlSplitFetcherManager extends SplitFetcherManager<RowData, MySqlSplit> {
    public MysqlSplitFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
                                    Supplier<SplitReader<RowData, MySqlSplit>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory);
    }

    @Override
    public void addSplits(List<MySqlSplit> splitsToAdd) {

    }
}
