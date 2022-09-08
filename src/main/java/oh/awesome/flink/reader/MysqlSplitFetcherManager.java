package oh.awesome.flink.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.split.MySqlSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MysqlSplitFetcherManager extends SplitFetcherManager<RowData, MySqlSplit> {
    public MysqlSplitFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
                                    Supplier<SplitReader<RowData, MySqlSplit>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory);

        int SplitFetcherNum = Integer.parseInt(ConfigOptions.SPLIT_FETCHER_NUM);
        for (int i = 0; i < SplitFetcherNum; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<MySqlSplit> splitsToAdd) {
        final Iterator<Integer>[] iterator = new Iterator[]{fetchers.keySet().iterator()};

        splitsToAdd.stream()
                .map(split -> {
                    if (!iterator[0].hasNext()) {
                        iterator[0] = fetchers.keySet().iterator();
                    }
                    return new Tuple2<>(iterator[0].next(), split);
                })
                .collect(HashMap::new,
                        (map, tuple2) -> {
                            int fetcherId = tuple2.f0;
                            map.computeIfAbsent(fetcherId, integer -> new ArrayList<>());
                            map.get(fetcherId).add(tuple2.f1);
                            },
                        (BiConsumer<Map<Integer, List<MySqlSplit>>, Map<Integer, List<MySqlSplit>>>) Map::putAll)
                .forEach((key, value) -> fetchers.get(key).addSplits(value));
    }
}
