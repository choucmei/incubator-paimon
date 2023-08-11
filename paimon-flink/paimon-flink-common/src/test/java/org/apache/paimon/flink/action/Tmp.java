package org.apache.paimon.flink.action;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction;
import org.apache.paimon.options.CatalogOptions;

import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Tmp {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        executionEnvironment.getCheckpointConfig().setCheckpointInterval(10 * 1000);
        Map<String, String> config = new HashMap<>();
        config.put("hostname", "tslave2");
        config.put("port", "3306");
        config.put("username", "root");
        config.put("password", "123456");
        // see mysql/my.cnf in test resources
        config.put("server-time-zone", ZoneId.of("UTC").toString());
        config.put("database-name", "test");
        config.put("table-name", "request_logs");
        Map<String, String> tableConfig = new HashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncTableAction action =
                new MySqlSyncTableAction(
                        config,
                        "/tmp/paimon0727",
                        "test",
                        "request_logs_part",
                        Arrays.asList("dt","ht"),
                        Arrays.asList("id","dt","ht"),
                        Collections.singletonMap(
                                CatalogOptions.METASTORE.key(), "test-alter-table"),
                        tableConfig);
        try {
            action.build(executionEnvironment);
            executionEnvironment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
