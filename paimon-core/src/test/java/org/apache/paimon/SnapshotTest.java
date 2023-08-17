package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

import static org.apache.paimon.memory.MemorySegmentUtils.copyToBytes;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

public class SnapshotTest {
    static final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());
    static String pathStr = "/tmp/paimon/default.db/tb_all_type/";
    static Path path = new Path(pathStr);
    static FileIO fileIO = FileIOFinder.find(path);

    public static void main(String[] args) {
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, path);
//        for (long i = snapshotManager.earliestSnapshotId(); i <= snapshotManager.latestSnapshotId(); i++) {
//            printSnapshot(snapshotManager.snapshot(i));
//        }
        printSnapshot(snapshotManager.snapshot(5));

    }

    public static void printSnapshot(Snapshot snapshot) {
        System.out.println("**********************************************************");
        System.out.println(snapshot.toJson());
        printManifestList(snapshot.baseManifestList());
        printManifestList(snapshot.deltaManifestList());
        System.out.println("**********************************************************");
    }

    public static void printManifestList(String manifestListFile) {
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString());
        ManifestList manifestList = new ManifestList.Factory(FileIOFinder.find(path), avro, pathFactory, null).create();

        List<ManifestFileMeta> read = manifestList.read(manifestListFile);
        System.out.println(" manifestListFile: " + manifestListFile + " ManifestFileMeta.size(): " + read.size());
        for (ManifestFileMeta manifestFileMeta : read) {
            System.out.println(manifestFileMeta);
            printMainFile(manifestFileMeta.fileName());
        }
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    }

    public static void printMainFile(String manifestFileStr) {
        System.out.println("-----------------------------------------------------------");
        ManifestFile manifestFile = new ManifestFile.Factory(fileIO,
                new SchemaManager(fileIO, path),
                RowType.of(new VarCharType(), new VarCharType()),
                avro,
                new FileStorePathFactory(
                        path,
                        RowType.of(new VarCharType(), new VarCharType()),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString()),
                Long.MAX_VALUE,
                null).create();
        List<ManifestEntry> read = manifestFile.read(manifestFileStr);
        System.out.println(" manifestFileStr: " + manifestFileStr + " ManifestEntry.size(): " + read.size());
        for (ManifestEntry manifestEntry : read) {
            System.out.println(manifestEntry);
            printBinaryRow(manifestEntry.partition());
        }
        System.out.println("-----------------------------------------------------------");
    }

    public static void printBinaryRow(BinaryRow binaryRow){
        int fieldCount = binaryRow.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            System.out.println(binaryRow.getString(i));
        }
    }
}
