package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.split.MySqlSplit;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class MysqlSplitFetcherManager extends SplitFetcherManager<RowData, MySqlSplit> {
    public MysqlSplitFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
                                    Supplier<SplitReader<RowData, MySqlSplit>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory);
    }

    @Override
    public void addSplits(List<MySqlSplit> splitsToAdd) {
        if (getSplitFetcherNum() >= Integer.parseInt(ConfigOptions.SPLIT_FETCHER_NUM)) {
            SplitFetcher<RowData, MySqlSplit> splitFetcher = createSplitFetcher();
            splitFetcher.addSplits(splitsToAdd);
            startFetcher(splitFetcher);
        } else {

        }
    }

    private int getSplitFetcherNum() {
        return fetchers.size();
    }

    private SplitFetcher<RowData, MySqlSplit> getMostIdleSplitFetcher() {
        return fetchers.entrySet().stream().min(new Comparator<Map.Entry<Integer, SplitFetcher<RowData, MySqlSplit>>>() {
            @Override
            public int compare(Map.Entry<Integer, SplitFetcher<RowData, MySqlSplit>> first, Map.Entry<Integer, SplitFetcher<RowData, MySqlSplit>> second) {
                return 0;
            }
        }).get().getValue();
    }
}
