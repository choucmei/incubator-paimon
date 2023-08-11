package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class JavaApi {
    public static void main(String[] args) throws Exception {
        Catalog catalog = getCatalog();
        Identifier identifier = Identifier.create("default", "tb_all_type");
        Table table = catalog.getTable(identifier);
//        write(table);
        read(table);
    }

    public static Catalog getCatalog() {
        CatalogContext context = CatalogContext.create(new Path("file:///tmp/paimon/"));
        Catalog catalog = CatalogFactory.createCatalog(context);
        return catalog;
    }

    public void creatTable(Catalog catalog) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("id", "dt", "hr");
        schemaBuilder.partitionKeys("dt", "hr");
        schemaBuilder.column("p", DataTypes.TIME());
        schemaBuilder.column("p1", DataTypes.TIME(1));
        schemaBuilder.column("p6", DataTypes.TIME(6));
        schemaBuilder.column("p9", DataTypes.TIME(9));
        schemaBuilder.column("q1", DataTypes.TIMESTAMP(1));
        schemaBuilder.column("q6", DataTypes.TIMESTAMP(6));
        schemaBuilder.column("q9", DataTypes.TIMESTAMP(9));
        schemaBuilder.column("r1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(1));
        schemaBuilder.column("r6", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
        schemaBuilder.column("r9", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9));
        schemaBuilder.column("dt", DataTypes.STRING());
        schemaBuilder.column("hr", DataTypes.STRING());
        schemaBuilder.column("id", DataTypes.BIGINT());
        Schema schema = schemaBuilder.build();
        Identifier identifier = Identifier.create("default.db", "my_table");
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
            e.printStackTrace();
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
            e.printStackTrace();
        }
    }

    public static void write(Table table) throws Exception {
        Map<String, String> partitions = new HashMap<>();
        partitions.put("dt", "20230808");
        partitions.put("hr", "1449");
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder()
                .withOverwrite(partitions);

        // 2. Write records in distributed tasks
        BatchTableWrite write = writeBuilder.newWrite();


        GenericRow record1 = GenericRow.of(37790, 37790, 37790, 37790,
                Timestamp.fromEpochMillis(1691478599000l), Timestamp.fromEpochMillis(1691478599000l), Timestamp.fromEpochMillis(1691478599000l),
                Timestamp.fromEpochMillis(1691478599000l), Timestamp.fromEpochMillis(1691478599000l), Timestamp.fromEpochMillis(1691478599000l),
                BinaryString.fromString("20230808"), BinaryString.fromString("1500"), 11l);
//        GenericRow record2 = GenericRow.of(1,1,1,1,1,1,1,1,1,1,1,1,BinaryString.fromString("20230808"),BinaryString.fromString("1501"),22l);
//        GenericRow record3 = GenericRow.of(1,1,1,1,1,1,1,1,1,1,1,1,BinaryString.fromString("20230808"),BinaryString.fromString("1502"),22l);

        write.write(record1);
//        write.write(record2);
//        write.write(record3);

        List<CommitMessage> messages = write.prepareCommit();

        // 3. Collect all CommitMessages to a global node and commit
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
    }

    public static void read(Table table) throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        List<Split> splits = readBuilder.newScan().plan().splits();

        // 3. Distribute these splits to different tasks

        // 4. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(new Consumer<InternalRow>() {
            @Override
            public void accept(InternalRow internalRow) {
//                int fieldCount = internalRow.getFieldCount();
//                System.out.println(internalRow.getInt(0));
//                System.out.println(internalRow.getInt(1));
//                System.out.println(internalRow.getInt(2));
//                System.out.println(internalRow.getInt(3));
                System.out.println(internalRow);
            }
        });
    }
}
