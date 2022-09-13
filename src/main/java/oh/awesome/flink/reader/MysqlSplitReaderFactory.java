package oh.awesome.flink.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import oh.awesome.flink.config.MysqlSnapshotSourceOptions;
import oh.awesome.flink.converter.MySQLRowConverter;
import oh.awesome.flink.converter.MysqlDataType;
import oh.awesome.flink.dialect.MySQLDialect;
import oh.awesome.flink.split.MySqlSplit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.function.Supplier;

public class MysqlSplitReaderFactory implements Supplier<SplitReader<RowData, MySqlSplit>> {
    private final Connection connection;
    private final MySQLRowConverter converter;

    public MysqlSplitReaderFactory(Configuration configuration) throws Exception {
        this.connection = DriverManager.getConnection(
                configuration.getString(MysqlSnapshotSourceOptions.HOST),
                configuration.getString(MysqlSnapshotSourceOptions.USERNAME),
                configuration.getString(MysqlSnapshotSourceOptions.PASSWORD)
        );

        ResultSetMetaData resultSetMetaData = connection.prepareStatement(
                MySQLDialect.getSelectFromStatement(
                        configuration.getString(MysqlSnapshotSourceOptions.SCHEMA_NAME),
                        configuration.getString(MysqlSnapshotSourceOptions.TABLE_NAME),
                        configuration.getOptional(MysqlSnapshotSourceOptions.SOURCE_READER_PROJECT_COLUMNS)
                        .orElse(MysqlSnapshotSourceOptions.SOURCE_READER_PROJECT_COLUMNS.defaultValue())
                )).getMetaData();

        DataTypes.Field[] fields = new DataTypes.Field[resultSetMetaData.getColumnCount()];
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            fields[i - 1] = DataTypes.FIELD(resultSetMetaData.getColumnName(i), MysqlDataType.fromJDBCType(resultSetMetaData, i));
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
