package oh.awesome.flink.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.split.MySqlSplit;
import oh.awesome.flink.split.MySqlSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

public class MysqlSourceReader extends SourceReaderBase<RowData, RowData, MySqlSplit, MySqlSplitState> {

    private final Connection connection;

    private static final Logger LOG = LoggerFactory.getLogger(MysqlSourceReader.class);

    public MysqlSourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
                             SplitFetcherManager<RowData, MySqlSplit> splitFetcherManager,
                             RecordEmitter<RowData, RowData, MySqlSplitState> recordEmitter,
                             Configuration config,
                             SourceReaderContext context,
                             Connection connection) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.connection = connection;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        finishedSplitIds.keySet().forEach(split -> LOG.info("split {} read finished", split));
    }

    @Override
    protected MySqlSplitState initializedState(MySqlSplit split) {
        return new MySqlSplitState(split);
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
