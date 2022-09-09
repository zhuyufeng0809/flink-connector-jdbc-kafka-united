package oh.awesome.flink;

import oh.awesome.flink.config.MysqlSnapshotSourceOptions;
import oh.awesome.flink.enumerator.MySqlSplitEnumerator;
import oh.awesome.flink.enumerator.MysqlSplitEnumeratorStateSerializer;
import oh.awesome.flink.reader.MysqlRecordEmitter;
import oh.awesome.flink.reader.MysqlSourceReader;
import oh.awesome.flink.reader.MysqlSplitFetcherManager;
import oh.awesome.flink.reader.MysqlSplitReaderFactory;
import oh.awesome.flink.split.MySqlSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.enumerator.MysqlSplitEnumeratorState;
import oh.awesome.flink.split.MySqlSplit;

import java.util.List;

/**
 * only for snapshot reading in hybrid source
 */
public class MysqlSnapshotSource implements Source<RowData, MySqlSplit, MysqlSplitEnumeratorState>, ResultTypeQueryable<RowData> {
    private final Configuration configuration;

    public MysqlSnapshotSource(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, MySqlSplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue =
                new FutureCompletingBlockingQueue<>(configuration.getInteger(MysqlSnapshotSourceOptions.SOURCE_READER_QUEUE_CAPACITY));
        MysqlSplitReaderFactory mysqlSplitReaderFactory = new MysqlSplitReaderFactory(configuration.clone());
        MysqlSplitFetcherManager mysqlSplitFetcherManager = new MysqlSplitFetcherManager(
                elementsQueue,
                mysqlSplitReaderFactory,
                configuration.clone()
        );
        return new MysqlSourceReader(
                elementsQueue,
                mysqlSplitFetcherManager,
                new MysqlRecordEmitter(),
                configuration.clone(),
                readerContext,
                mysqlSplitReaderFactory.getConnection()
        );
    }

    @Override
    public SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> createEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext) throws Exception {
        return new MySqlSplitEnumerator(enumContext, configuration.clone());
    }

    @Override
    public SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> restoreEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext, MysqlSplitEnumeratorState checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return new MySqlSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<MysqlSplitEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new MysqlSplitEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    public static MysqlSnapshotSourceBuilder builder() {
        return new MysqlSnapshotSourceBuilder();
    }

    public static class MysqlSnapshotSourceBuilder {
        private final Configuration configuration;

        public MysqlSnapshotSourceBuilder() {
            this.configuration = new Configuration();
        }

        public MysqlSnapshotSourceBuilder host(String host) {
            configuration.setString(MysqlSnapshotSourceOptions.HOST, host);
            return this;
        }

        public MysqlSnapshotSourceBuilder port(Integer port) {
            configuration.setInteger(MysqlSnapshotSourceOptions.PORT, port);
            return this;
        }

        public MysqlSnapshotSourceBuilder username(String username) {
            configuration.setString(MysqlSnapshotSourceOptions.USERNAME, username);
            return this;
        }

        public MysqlSnapshotSourceBuilder password(String password) {
            configuration.setString(MysqlSnapshotSourceOptions.PASSWORD, password);
            return this;
        }

        public MysqlSnapshotSourceBuilder schema(String schema) {
            configuration.setString(MysqlSnapshotSourceOptions.SCHEMA_NAME, schema);
            return this;
        }

        public MysqlSnapshotSourceBuilder table(String table) {
            configuration.setString(MysqlSnapshotSourceOptions.TABLE_NAME, table);
            return this;
        }

        public MysqlSnapshotSourceBuilder splitColumn(String splitColumn) {
            configuration.setString(MysqlSnapshotSourceOptions.SPLIT_COLUMN, splitColumn);
            return this;
        }

        public MysqlSnapshotSourceBuilder splitNum(Integer splitNum) {
            configuration.setInteger(MysqlSnapshotSourceOptions.SPLIT_NUM, splitNum);
            return this;
        }

        public MysqlSnapshotSourceBuilder sourceReaderQueueCapacity(Integer sourceReaderQueueCapacity) {
            configuration.setInteger(MysqlSnapshotSourceOptions.SOURCE_READER_QUEUE_CAPACITY, sourceReaderQueueCapacity);
            return this;
        }

        public MysqlSnapshotSourceBuilder splitFetcherNum(Integer splitFetcherNum) {
            configuration.setInteger(MysqlSnapshotSourceOptions.SOURCE_READER_SPLIT_FETCHER_NUM, splitFetcherNum);
            return this;
        }

        public MysqlSnapshotSourceBuilder project(List<String> fieldNames) {
            configuration.set(MysqlSnapshotSourceOptions.SOURCE_READER_PROJECT_COLUMNS, fieldNames);
            return this;
        }

        public MysqlSnapshotSource build() {
            return new MysqlSnapshotSource(configuration);
        }
    }
}
