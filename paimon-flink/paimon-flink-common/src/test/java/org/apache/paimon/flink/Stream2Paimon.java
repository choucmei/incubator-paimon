package org.apache.paimon.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Stream2Paimon {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getCheckpointConfig().setCheckpointInterval(1000 * 30);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type'='paimon',\n" +
                "    'warehouse'='file:/tmp/paimon'\n" +
                ")\n").print();
        streamTableEnvironment.executeSql("CREATE TABLE `paimon_catalog`.`default`.`tb_all_type` (\n" +
                "  id BIGINT,\n" +
                "    a CHAR,\n" +
                "    b VARCHAR,\n" +
                "    c STRING,\n" +
                "    d BOOLEAN,\n" +
//                "    e BINARY,\n" +
//                "    f VARBINARY,\n" +
//                "    g BYTES,\n" +
                "    h DECIMAL(10,2),\n" +
                "    i TINYINT,\n" +
                "    j SMALLINT,\n" +
                "    k INTEGER,\n" +
                "    l BIGINT,\n" +
                "    m FLOAT,\n" +
                "    n DOUBLE,\n" +
                "    o DATE,\n" +
                "    q TIMESTAMP,\n" +
                "    q0 TIMESTAMP(0),\n" +
                "    q3 TIMESTAMP(3),\n" +
                "    q6 TIMESTAMP(6),\n" +
                "    q12 TIMESTAMP(12),\n" +
                "    q13 TIMESTAMP(13),\n" +
                "    r TIMESTAMP_LTZ,\n" +
                "    r0 TIMESTAMP_LTZ(0),\n" +
                "    r3 TIMESTAMP_LTZ(3),\n" +
                "    r6 TIMESTAMP_LTZ(6),\n" +
                "    r12 TIMESTAMP_LTZ(12),\n" +
                "    r13 TIMESTAMP_LTZ(13),\n" +
                "  dt STRING COMMENT 'timestamp string in format yyyyMMdd',\n" +
                "  hr STRING COMMENT 'timestamp string in format HHmm',\n" +
                "  PRIMARY KEY(id, dt, hr) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt, hr)").print();
        streamTableEnvironment.executeSql("use catalog paimon_catalog").print();
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE GenOrders\n" +
                "WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '100'\n" +
                ")\n" +
                "LIKE `paimon_catalog`.`default`.`tb_all_type` (EXCLUDING ALL)\n").print();
//        streamTableEnvironment.executeSql("alter table `paimon_catalog`.`default`.`tb_all_type` partition (dt='1111', hr='222')").print();
        streamTableEnvironment.executeSql("insert into `paimon_catalog`.`default`.`tb_all_type` select id,a,b,c,d,h,i,j,k,l,m,n,o,q,q0,q3,q6,q12,q13,r,r0,r3,r6,r12,r13,DATE_FORMAT(CURRENT_ROW_TIMESTAMP(),'yyyyMMdd'),DATE_FORMAT(CURRENT_ROW_TIMESTAMP(),'HHmm') from GenOrders").print();
//        streamTableEnvironment.executeSql("desc `paimon_catalog`.`default`.`tb_all_type` ").print();
//
//        streamTableEnvironment.executeSql("select * from `paimon_catalog`.`default`.`tb_all_type` limit 10").print();
    }
}
