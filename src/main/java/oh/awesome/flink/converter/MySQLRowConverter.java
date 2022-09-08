package oh.awesome.flink.converter;

import org.apache.flink.table.types.logical.RowType;

public class MySQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "MySQL";
    }

    public MySQLRowConverter(RowType rowType) {
        super(rowType);
    }
}
