package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import oh.awesome.flink.config.ConfigOptions;
import oh.awesome.flink.converter.MySQLRowConverter;
import oh.awesome.flink.converter.MysqlDataType;
import oh.awesome.flink.dialect.MySQLDialect;
import oh.awesome.flink.split.MySqlSplit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import java.util.function.Supplier;

public class MysqlSplitReaderFactory implements Supplier<SplitReader<RowData, MySqlSplit>> {
    private final Connection connection;
    private final MySQLRowConverter converter;

    public MysqlSplitReaderFactory(Properties properties, String[] fieldNames) throws Exception {
        this.connection = DriverManager.getConnection(
                properties.getProperty(ConfigOptions.HOST),
                properties.getProperty(ConfigOptions.USERNAME),
                properties.getProperty(ConfigOptions.PASSWORD)
        );

        ResultSetMetaData resultSetMetaData = connection.prepareStatement(
                MySQLDialect.getSelectFromStatement(
                        properties.getProperty(ConfigOptions.SCHEMA),
                        properties.getProperty(ConfigOptions.TABLE),
                        fieldNames
                )).getMetaData();

        DataTypes.Field[] fields = new DataTypes.Field[resultSetMetaData.getColumnCount()];
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            fields[i] = DataTypes.FIELD(resultSetMetaData.getColumnName(i), MysqlDataType.fromJDBCType(resultSetMetaData, i));
        }

        final RowType rowType = (RowType) DataTypes.ROW(fields).notNull().getLogicalType();
        this.converter = new MySQLRowConverter(rowType);
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public SplitReader<RowData, MySqlSplit> get() {
        return new MysqlSplitReader(connection, converter);
    }
}
