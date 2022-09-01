package oh.awesome.flink;

public class App {
    public static void main(String[] args) {
        MysqlSnapshotSource mysqlSnapshotSource = MysqlSnapshotSource.builder()
                .host("")
                .port(3306)
                .schema("")
                .table("")
                .username("")
                .password("")
                .splitColumn("")
                .build();
    }
}
