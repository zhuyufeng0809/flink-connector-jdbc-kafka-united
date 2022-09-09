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
                .splitNum(64)
                .sourceReaderQueueCapacity(32)
                .splitFetcherNum(16)
                .build();
    }
}
