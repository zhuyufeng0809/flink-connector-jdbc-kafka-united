package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.split.MySqlSplit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.function.Supplier;

public class MysqlSplitReaderFactory implements Supplier<SplitReader<RowData, MySqlSplit>> {
    private final Connection connection;

    public MysqlSplitReaderFactory(Properties properties) throws Exception {
        connection = DriverManager.getConnection(
                properties.getProperty(ConfigOptions.HOST),
                properties.getProperty(ConfigOptions.USERNAME),
                properties.getProperty(ConfigOptions.PASSWORD)
        );
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public SplitReader<RowData, MySqlSplit> get() {
        return null;
    }
}
