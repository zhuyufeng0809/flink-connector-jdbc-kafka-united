package oh.awesome.flink.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MysqlDataType {
    private final static String MYSQL_TINYINT = "TINYINT";
    private final static String MYSQL_SMALLINT = "SMALLINT";
    private final static String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private final static String MYSQL_INT = "INT";
    private final static String MYSQL_MEDIUMINT = "MEDIUMINT";
    private final static String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private final static String MYSQL_BIGINT = "BIGINT";
    private final static String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    private final static String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private final static String MYSQL_FLOAT = "FLOAT";
    private final static String MYSQL_DOUBLE = "DOUBLE";
    private final static String MYSQL_DECIMAL = "DECIMAL";
    private final static String MYSQL_BOOLEAN = "BOOLEAN";
    private final static String MYSQL_DATE = "DATE";
    private final static String MYSQL_TIME = "TIME";
    private final static String MYSQL_DATETIME  = "DATETIME";
    private final static String MYSQL_CHAR = "CHAR";
    private final static String MYSQL_VARCHAR = "VARCHAR";
    private final static String MYSQL_TEXT = "TEXT";
    private final static String MYSQL_BINARY = "BINARY";
    private final static String MYSQL_VARBINARY = "VARBINARY";
    private final static String MYSQL_BLOB = "BLOB";
    //support PolarDb Mysql type 'BIT'
    private final static String MYSQL_BIT = "BIT";
    //support PolarDb Mysql type 'TIMESTAMP'
    private final static String MYSQL_TIMESTAMP = "TIMESTAMP";

    public static DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        //reference Flink official website
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/jdbc/#data-type-mapping
        switch (mysqlType) {
            case MYSQL_TINYINT:
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case MYSQL_SMALLINT:
            case MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MYSQL_INT:
            case MYSQL_MEDIUMINT:
            case MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_BIGINT:
            case MYSQL_INT_UNSIGNED:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_BOOLEAN:
                return DataTypes.BOOLEAN();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case MYSQL_CHAR:
            case MYSQL_VARCHAR:
            case MYSQL_TEXT:
                //type 'BIT' reference PolarDb Mysql official website
                //https://help.aliyun.com/document_detail/131282.htm?spm=a2c4g.11186623.0.0.4f86739cjdzqAQ#concept-1813784
            case MYSQL_BIT:
                return DataTypes.STRING();
            case MYSQL_BINARY:
            case MYSQL_VARBINARY:
            case MYSQL_BLOB:
                return DataTypes.BYTES();
            default:
                String columnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Mysql type '%s' of column '%s' yet, please contact author", mysqlType,columnName));
        }
    }
}
