package oh.awesome.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import oh.awesome.flink.config.KafkaMessageSourceOptions;
import oh.awesome.flink.config.MysqlSnapshotSourceOptions;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnifySourceDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "hybrid";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(
                MysqlSnapshotSourceOptions.HOST,
                MysqlSnapshotSourceOptions.PORT,
                MysqlSnapshotSourceOptions.USERNAME,
                MysqlSnapshotSourceOptions.PASSWORD,
                MysqlSnapshotSourceOptions.SCHEMA_NAME,
                MysqlSnapshotSourceOptions.TABLE_NAME,
                MysqlSnapshotSourceOptions.SPLIT_COLUMN,
                KafkaMessageSourceOptions.TOPIC,
                KafkaMessageSourceOptions.TOPIC_PATTERN,
                KafkaMessageSourceOptions.PROPS_BOOTSTRAP_SERVERS,
                KafkaMessageSourceOptions.PROPS_GROUP_ID,
                KafkaMessageSourceOptions.VALUE_FORMAT
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                MysqlSnapshotSourceOptions.SPLIT_NUM,
                MysqlSnapshotSourceOptions.SOURCE_READER_QUEUE_CAPACITY,
                MysqlSnapshotSourceOptions.SOURCE_READER_SPLIT_FETCHER_NUM,
                MysqlSnapshotSourceOptions.SOURCE_READER_PROJECT_COLUMNS
        ).collect(Collectors.toSet());
    }
}
