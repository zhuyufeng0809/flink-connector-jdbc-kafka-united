package oh.awesome.flink;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.enumerator.MySqlSplitEnumerator;
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

import java.util.Properties;

/**
 * only for snapshot reading in hybrid source
 */
public class MysqlSnapshotSource implements Source<RowData, MySqlSplit, MysqlSplitEnumeratorState>, ResultTypeQueryable<RowData> {
    private final Properties config;
    private final String[] fieldNames;

    private MysqlSnapshotSource(Properties config, String[] fieldNames) {
        this.config = config;
        this.fieldNames = fieldNames;
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
        Properties properties = new Properties();
        properties.putAll(config);
        return new MySqlSplitEnumerator(enumContext, properties);
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
        private final Properties properties;
        private String[] fieldNames;

        public MysqlSnapshotSourceBuilder() {
            this.properties = new Properties();
        }

        public MysqlSnapshotSourceBuilder host(String host) {
            properties.setProperty(ConfigOptions.HOST, host);
            return this;
        }

        public MysqlSnapshotSourceBuilder port(Integer port) {
            properties.setProperty(ConfigOptions.PORT, port.toString());
            return this;
        }

        public MysqlSnapshotSourceBuilder username(String username) {
            properties.setProperty(ConfigOptions.USERNAME, username);
            return this;
        }

        public MysqlSnapshotSourceBuilder password(String password) {
            properties.setProperty(ConfigOptions.PASSWORD, password);
            return this;
        }

        public MysqlSnapshotSourceBuilder schema(String schema) {
            properties.setProperty(ConfigOptions.SCHEMA, schema);
            return this;
        }

        public MysqlSnapshotSourceBuilder table(String table) {
            properties.setProperty(ConfigOptions.TABLE, table);
            return this;
        }

        public MysqlSnapshotSourceBuilder splitColumn(String splitColumn) {
            properties.setProperty(ConfigOptions.SPLIT_COLUMN, splitColumn);
            return this;
        }

        public MysqlSnapshotSourceBuilder splitNum(Integer splitNum) {
            properties.setProperty(ConfigOptions.SPLIT_NUM, splitNum.toString());
            return this;
        }

        public MysqlSnapshotSourceBuilder project(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public MysqlSnapshotSource build() {
            if (fieldNames == null) {
                fieldNames = new String[]{"*"};
            }
            return new MysqlSnapshotSource(properties, fieldNames);
        }
    }
}
