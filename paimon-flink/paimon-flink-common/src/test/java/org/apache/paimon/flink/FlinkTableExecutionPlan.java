package org.apache.paimon.flink;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chouc
 * @version V1.0
 * @Title: FlinkTableDemo01
 * @Package com.chouc.flink.table
 * @Description:
 * @date 2021/4/6
 */
public class FlinkTableExecutionPlan {
    public static class WC {
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        DataStreamSource<WC> source = environment.fromElements(new WC("hello", 1), new WC("hello", 1), new WC("world", 1));
        tableEnvironment.createTemporaryView("wc", source);
        Table selectedTable = tableEnvironment.sqlQuery("select * from wc");
        selectedTable.printSchema();
        selectedTable.execute().print();
        System.out.println("ExplainDetail.CHANGELOG_MODE");
        System.out.println(selectedTable.explain(ExplainDetail.CHANGELOG_MODE));
        System.out.println("ExplainDetail.ESTIMATED_COST");
        System.out.println(selectedTable.explain(ExplainDetail.ESTIMATED_COST));

    }
}
