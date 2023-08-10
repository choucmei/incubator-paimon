package org.apache.paimon.flink.action;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.sink.*;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;

public class FlinkActionToMysql {
    public static void main(String[] args) throws Exception {
//        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path("~/tmp_data/paimon");
//        Options conf = new Options();
//        conf.set(CoreOptions.PATH, tablePath.toString());
//        TableSchema tableSchema =
//                SchemaUtils.forceCommit(
//                        new SchemaManager(LocalFileIO.create(), tablePath),
//                        new Schema(
//                                Arrays.asList(new DataField(1, "url", new VarCharType()),
//                                        new DataField(2, "ip", new VarCharType()),
//                                        new DataField(3, "threadName", new VarCharType()),
//                                        new DataField(4, "ts", new BigIntType()),
//                                        new DataField(5, "spent", new BigIntType()),
//                                        new DataField(6, "dt", new VarCharType()),
//                                        new DataField(7, "hr", new VarCharType())
//                                ),
//                                Arrays.asList("dt", "hr"),
//                                Arrays.asList("ts", "dt", "hr"),
//                                conf.toMap(),
//                                ""));
//        FileStoreTable fileStoreTable = FileStoreTableFactory.create(
//                FileIOFinder.find(tablePath),
//                tablePath,
//                tableSchema,
//                conf,
//                Lock.emptyFactory(),
//                null);
//        FlinkSink<RowData> flinkSink = new FileStoreSink(fileStoreTable, null, null);
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
//        DataStream<RowData> filter = executionEnvironment.readTextFile("/home/chouc/e/")
//                .<RowData>map(s -> {
//                    String[] s2 = s.substring(0, 31).split(" ");
//                    String[] s1 = s.substring(32).split(" ");
//                    String threadName = s1[0].substring(1, s1[0].length() - 1);
//                    long ts = Long.parseLong(s1[3]);
//                    String url = s1[4];
//                    String ip = s1[5];
//                    long spent;
//                    String dt = s2[0].replace("-", "");
//                    String hr = s2[0].replace(":", "");
//                    if (s1.length < 7) {
//                        spent = 0;
//                    } else {
//                        spent = Long.parseLong(s1[6].substring(0, s1[6].length() - 2));
//                    }
//                    return new FlinkRowData(GenericRow.of(url, ip, threadName, spent, ts, dt, hr));
//                }).filter(t -> t.getLong(4) > 0);
//        SingleOutputStreamOperator<Committable> written = flinkSink.doWrite(filter, "123", 1);
//        RowDataStoreWriteOperator operator =
//                ((RowDataStoreWriteOperator)
//                        ((SimpleOperatorFactory)
//                                ((OneInputTransformation) written.getTransformation())
//                                        .getOperatorFactory())
//                                .getOperator());
//        StateInitializationContextImpl context =
//                new StateInitializationContextImpl(
//                        null,
//                        new MockOperatorStateStore() {
//                            @Override
//                            public <S> ListState<S> getUnionListState(
//                                    ListStateDescriptor<S> stateDescriptor) throws Exception {
//                                return getListState(stateDescriptor);
//                            }
//                        },
//                        null,
//                        null,
//                        null);
//        operator.initStateAndWriter(context, (a, b, c) -> true, new IOManagerAsync(), "123");
//        return ((KeyValueFileStoreWrite) ((StoreSinkWriteImpl) operator.write).write.getWrite())
//                .bufferSpillable();
//        executionEnvironment.execute();
    }

}
