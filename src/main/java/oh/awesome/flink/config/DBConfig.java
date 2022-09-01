package oh.awesome.flink.config;

public class DBConfig {
    private final String host;
    private final Integer port;
    private final String username;
    private final String password;
    private final String schema;
    private final String table;
    private final String splitColumn;

    public DBConfig(String host, Integer port, String username, String password, String schema, String table, String splitColumn) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.schema = schema;
        this.table = table;
        this.splitColumn = splitColumn;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getSplitColumn() {
        return splitColumn;
    }
}
