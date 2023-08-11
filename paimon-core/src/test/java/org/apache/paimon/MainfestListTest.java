package org.apache.paimon;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.parquet.format.StringType;

import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

public class MainfestListTest {
    public static void main(String[] args) {
        final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());
        Path path = new Path("/tmp/paimon/default.db/tb_all_type/");
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString());
        ManifestList manifestList = new ManifestList.Factory(FileIOFinder.find(path), avro, pathFactory, null).create();
        List<ManifestFileMeta> read = manifestList.read("manifest-list-08682deb-f3a2-4539-bc82-87b2c43c9eac-0");
        for (ManifestFileMeta manifestFileMeta : read) {
            System.out.println(manifestFileMeta.fileName());
        }
        for (ManifestFileMeta manifestFileMeta : read) {
            System.out.println("***"+manifestFileMeta.fileName());
            ManifestFile aDefault = new ManifestFile.Factory(FileIOFinder.find(path),
                    new SchemaManager(FileIOFinder.find(path), path),
                    RowType.of(new VarCharType(),new VarCharType()),
                    avro,
                    new FileStorePathFactory(
                            path,
                            RowType.of(new VarCharType(),new VarCharType()),
                            "default",
                            CoreOptions.FILE_FORMAT.defaultValue().toString()),
                    Long.MAX_VALUE,
                    null).create();
            List<ManifestEntry> read1 = aDefault.read(manifestFileMeta.fileName());
            for (ManifestEntry manifestEntry : read1) {
                System.out.println(new String(serializeBinaryRow(manifestEntry.partition())));
                System.out.println(manifestEntry);
            }
            System.out.println("----------------------------------------------------------------------");
        }

        List<ManifestFileMeta> read2 = manifestList.read("manifest-list-08682deb-f3a2-4539-bc82-87b2c43c9eac-1");
        for (ManifestFileMeta manifestFileMeta : read2) {
            System.out.println(manifestFileMeta.fileName());
        }

        for (ManifestFileMeta manifestFileMeta : read2) {
            System.out.println("***"+manifestFileMeta.fileName());
            ManifestFile aDefault = new ManifestFile.Factory(FileIOFinder.find(path),
                    new SchemaManager(FileIOFinder.find(path), path),
                    RowType.of(new VarCharType(),new VarCharType()),
                    avro,
                    new FileStorePathFactory(
                            path,
                            RowType.of(new VarCharType(),new VarCharType()),
                            "default",
                            CoreOptions.FILE_FORMAT.defaultValue().toString()),
                    Long.MAX_VALUE,
                    null).create();
            List<ManifestEntry> read1 = aDefault.read(manifestFileMeta.fileName());
            for (ManifestEntry manifestEntry : read1) {
                System.out.println(new String(serializeBinaryRow(manifestEntry.partition())));
                System.out.println(manifestEntry);
            }
            System.out.println("----------------------------------------------------------------------");
        }

        System.out.println("---------------");

        ManifestFile aDefault = new ManifestFile.Factory(FileIOFinder.find(path),
                new SchemaManager(FileIOFinder.find(path), path),
                RowType.of(new VarCharType(),new VarCharType()),
                avro,
                new FileStorePathFactory(
                        path,
                        RowType.of(new VarCharType(),new VarCharType()),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString()),
                Long.MAX_VALUE,
                null).create();
        List<ManifestEntry> read1 = aDefault.read("manifest-857f2107-27b8-4ef4-883f-37a603df8ad5-0");
        for (ManifestEntry manifestEntry : read1) {
            System.out.println(new String(serializeBinaryRow(manifestEntry.partition())));
            System.out.println(manifestEntry);
        }
    }
}
