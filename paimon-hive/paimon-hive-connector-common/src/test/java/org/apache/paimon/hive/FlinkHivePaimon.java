package org.apache.paimon.hive;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FlinkHivePaimon {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        executionEnvironment.getCheckpointConfig().setCheckpointInterval(3000);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment, EnvironmentSettings.newInstance().build());
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "paimon");
        catalogProperties.put("metastore", "hive");
        catalogProperties.put("uri", "thrift://slave1:9083");
        catalogProperties.put("lock.enabled", "true");
        catalogProperties.put("warehouse", "hdfs://slave1:8020/user/hive/paimon");
        String createCatalog = String.join(
                "\n",
                "CREATE CATALOG paimon_hive_catalog WITH (",
                catalogProperties.entrySet().stream()
                        .map(
                                e ->
                                        String.format(
                                                "'%s' = '%s'",
                                                e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n")),
                ")");
        System.out.println(createCatalog);
        streamTableEnvironment.executeSql(createCatalog).print();
        streamTableEnvironment.executeSql("show databases").print();
        streamTableEnvironment.executeSql("show tables").print();
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE GenOrders\n" +
                "WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '100'\n" +
                ")\n" +
                "LIKE `paimon_hive_catalog`.`default`.`tb_all_type_without_zone` (EXCLUDING ALL)\n").print();
        streamTableEnvironment.executeSql("insert into `paimon_hive_catalog`.`default`.`tb_all_type_without_zone` select id,a,b,c,d,h,i,j,k,l,m,n,o,q,q0,q3,q6,q12,q13,DATE_FORMAT(CURRENT_ROW_TIMESTAMP(),'yyyyMMdd'),DATE_FORMAT(CURRENT_ROW_TIMESTAMP(),'HHmm') from GenOrders").print();

//        streamTableEnvironment.executeSql("select * from `paimon_hive_catalog`.`default`.`tb_all_type` ").print();

//        streamTableEnvironment.executeSql("delete from `paimon_hive_catalog`.`default`.`tb_all_type` where dt='20230810' and hr='1127' ").print();
//        streamTableEnvironment.executeSql("select * from `paimon_hive_catalog`.`default`.`tb_all_type` where dt='20230810' and hr='1127' ").print();

//
//        streamTableEnvironment.executeSql("CREATE TABLE `paimon_hive_catalog`.`default`.`tb_all_type_without_zone` (\n" +
//                "  id BIGINT,\n" +
//                "    a CHAR,\n" +
//                "    b VARCHAR,\n" +
//                "    c STRING,\n" +
//                "    d BOOLEAN,\n" +
//                "    h DECIMAL(10,2),\n" +
//                "    i TINYINT,\n" +
//                "    j SMALLINT,\n" +
//                "    k INTEGER,\n" +
//                "    l BIGINT,\n" +
//                "    m FLOAT,\n" +
//                "    n DOUBLE,\n" +
//                "    o DATE,\n" +
//                "    q TIMESTAMP,\n" +
//                "    q0 TIMESTAMP(0),\n" +
//                "    q3 TIMESTAMP(3),\n" +
//                "    q6 TIMESTAMP(6),\n" +
//                "    q12 TIMESTAMP(12),\n" +
//                "    q13 TIMESTAMP(13),\n" +
//                "  dt STRING COMMENT 'timestamp string in format yyyyMMdd',\n" +
//                "  hr STRING COMMENT 'timestamp string in format HHmm',\n" +
//                "  PRIMARY KEY(id, dt, hr) NOT ENFORCED\n" +
//                ") PARTITIONED BY (dt, hr)").print();
    }
}
