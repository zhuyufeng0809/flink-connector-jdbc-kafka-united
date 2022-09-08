package oh.awesome.flink.converter;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param resultSet ResultSet from JDBC
     */
    RowData toInternal(ResultSet resultSet) throws SQLException;
}
