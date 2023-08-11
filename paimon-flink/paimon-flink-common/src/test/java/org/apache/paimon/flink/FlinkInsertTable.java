package org.apache.paimon.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkInsertTable {

    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/home/chouc/RequestLog/e/");
        SingleOutputStreamOperator<Tuple5<String, String, String, Long, Long>> filter = stringDataStreamSource.map(s -> {
            String[] s1 = s.substring(32).split(" ");
            String threadName = s1[0].substring(1, s1[0].length() - 1);
            Long timestamp = Long.parseLong(s1[3]);
            String url = s1[4];
            String ip = s1[5];
            Long spend;
            if (s1.length < 7) {
                spend = 0l;
            } else {
                spend = Long.parseLong(s1[6].substring(0, s1[6].length() - 2));
            }
            return Tuple5.of(threadName, url, ip, timestamp, spend);
        }).returns(new TypeHint<Tuple5<String, String, String, Long, Long>>() {
        }).filter(t -> t.f4 > 0);


        Schema.Builder builder = Schema.newBuilder();
        builder.column("f0", "String");
        builder.column("url", "String");
        builder.column("ip", "String");
        builder.column("timestamp", "Long");
        builder.column("expend", "Long");
        streamTableEnvironment.createTemporaryView("request", filter);

        streamTableEnvironment.executeSql("CREATE TABLE request_logs (  \n" +
                "  threadName STRING,  \n" +
                "  url STRING,  \n" +
                "  ip STRING,  \n" +
                "  `timestamp` BIGINT,  \n" +
                "  expend BIGINT,  \n" +
                "  dt STRING,  \n" +
                "  ht STRING  \n" +
                ")  \n" +
                "WITH (  \n" +
                "  'connector' = 'jdbc',  \n" +
                "  'url' = 'jdbc:mysql://tslave2:3306/test',  \n" +
                "  'table-name' = 'request_logs',  \n" +
                "  'username' = 'root',  \n" +
                "  'password' = '123456'  \n" +
                ");");
        streamTableEnvironment.executeSql("desc request").print();
//        streamTableEnvironment.executeSql("select f0,f1,f2,f3,f4,DATE_FORMAT(FROM_UNIXTIME(f3/1000),'YYYYMMDD'),DATE_FORMAT(FROM_UNIXTIME(f3/1000),'HH') from request").print();
        streamTableEnvironment.executeSql("insert into request_logs select f0,f1,f2,f3,f4,DATE_FORMAT(FROM_UNIXTIME(f3/1000),'yyyyMMdd'),DATE_FORMAT(FROM_UNIXTIME(f3/1000),'HH') from request").print();
    }
}
