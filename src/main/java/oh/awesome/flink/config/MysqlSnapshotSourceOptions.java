package oh.awesome.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

public class MysqlSnapshotSourceOptions {

    // -----------------------------------------------------------------------------------------
    // Required
    // -----------------------------------------------------------------------------------------
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC database host.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("The JDBC database port.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC user name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC password.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC schema name.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC table name.");
    public static final ConfigOption<String> SPLIT_COLUMN =
            ConfigOptions.key("split-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The column used for split.");

    // -----------------------------------------------------------------------------------------
    // Optional
    // -----------------------------------------------------------------------------------------
    public static final ConfigOption<Integer> SPLIT_NUM =
            ConfigOptions.key("enumerator.split-num")
                    .intType()
                    .defaultValue(64)
                    .withDescription("The number of split.");

    public static final ConfigOption<Integer> SOURCE_READER_QUEUE_CAPACITY =
            ConfigOptions.key("source-reader.queue-capacity")
                    .intType()
                    .defaultValue(64)
                    .withDescription("The queue capacity of SourceReader");

    public static final ConfigOption<Integer> SOURCE_READER_SPLIT_FETCHER_NUM =
            ConfigOptions.key("source-reader.split-fetcher-num")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The number of split fetcher.");

    public static final ConfigOption<List<String>> SOURCE_READER_PROJECT_COLUMNS =
            ConfigOptions.key("source-reader.project-columns")
                    .stringType()
                    .asList()
                    .defaultValues("*")
                    .withDescription("The table columns name");

    private MysqlSnapshotSourceOptions() {
    }
}
