package oh.awesome.flink;

import oh.awesome.flink.config.DBConfig;
import oh.awesome.flink.enumerator.MysqlSplitEnumeratorStateSerializer;
import oh.awesome.flink.split.MySqlSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.enumerator.MysqlSplitEnumeratorState;
import oh.awesome.flink.split.MySqlSplit;

/**
 * only for snapshot reading in hybrid source
 */
public class MysqlSnapshotSource implements Source<RowData, MySqlSplit, MysqlSplitEnumeratorState>, ResultTypeQueryable<RowData> {
    private final DBConfig config;

    private MysqlSnapshotSource(DBConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, MySqlSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> createEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext) throws Exception {
        return null;
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
        private String host;
        private Integer port;
        private String username;
        private String password;
        private String schema;
        private String table;
        private String splitColumn;

        public MysqlSnapshotSourceBuilder host(String host) {
            this.host = host;
            return this;
        }

        public MysqlSnapshotSourceBuilder port(Integer port) {
            this.port = port;
            return this;
        }

        public MysqlSnapshotSourceBuilder username(String username) {
            this.username = username;
            return this;
        }

        public MysqlSnapshotSourceBuilder password(String password) {
            this.password = password;
            return this;
        }

        public MysqlSnapshotSourceBuilder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public MysqlSnapshotSourceBuilder table(String table) {
            this.table = table;
            return this;
        }

        public MysqlSnapshotSourceBuilder splitColumn(String splitColumn) {
            this.splitColumn = splitColumn;
            return this;
        }

        public MysqlSnapshotSource build() {
            DBConfig config = new DBConfig(
                    host,
                    port,
                    username,
                    password,
                    schema,
                    table,
                    splitColumn);
            return new MysqlSnapshotSource(config);
        }
    }
}
