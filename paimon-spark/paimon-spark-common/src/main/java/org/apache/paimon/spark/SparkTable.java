/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.*;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A spark {@link org.apache.spark.sql.connector.catalog.Table} for paimon.
 */
public class SparkTable
        implements org.apache.spark.sql.connector.catalog.Table,
        SupportsRead,
        SupportsWrite,
        SupportsDelete,
        SupportsPartitionManagement {

    private final Table table;

    public SparkTable(Table table) {
        this.table = table;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        Table newTable = table.copy(options.asCaseSensitiveMap());
        return new SparkScanBuilder(newTable);
    }

    @Override
    public String name() {
        return table.name();
    }

    @Override
    public StructType schema() {
        return SparkTypeUtils.fromPaimonRowType(table.rowType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.V1_BATCH_WRITE);
        capabilities.add(TableCapability.OVERWRITE_BY_FILTER);
        return capabilities;
    }

    @Override
    public Transform[] partitioning() {
        return table.partitionKeys().stream()
                .map(FieldReference::apply)
                .map(IdentityTransform::apply)
                .toArray(Transform[]::new);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        try {
            return new SparkWriteBuilder((FileStoreTable) table, Options.fromMap(info.options()));
        } catch (Exception e) {
            throw new RuntimeException("Only FileStoreTable can be written.");
        }
    }

    @Override
    public void deleteWhere(Filter[] filters) {
        SparkFilterConverter converter = new SparkFilterConverter(table.rowType());
        List<Predicate> predicates = new ArrayList<>();
        for (Filter filter : filters) {
            if ("AlwaysTrue()".equals(filter.toString())) {
                continue;
            }

            predicates.add(converter.convert(filter));
        }

        TableUtils.deleteWhere(table, predicates);
    }

    @Override
    public Map<String, String> properties() {
        if (table instanceof DataTable) {
            Map<String, String> properties =
                    new HashMap<>(((DataTable) table).coreOptions().toMap());
            if (table.primaryKeys().size() > 0) {
                properties.put(
                        CoreOptions.PRIMARY_KEY.key(), String.join(",", table.primaryKeys()));
            }
            return properties;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public StructType partitionSchema() {
        List<String> partitionKeys = table.partitionKeys();
        RowType rowType = new RowType(table.rowType().getFields().stream().filter(dataField -> partitionKeys.contains(dataField.name())).collect(Collectors.toList()));
        return SparkTypeUtils.fromPaimonRowType(rowType);
    }

    @Override
    public void createPartition(InternalRow internalRow, Map<String, String> map) throws PartitionsAlreadyExistException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropPartition(InternalRow internalRow) {
        StructType structType = partitionSchema();
        StructField[] fields = structType.fields();
        HashMap<String, String> partitions = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            partitions.put(fields[i].name(), String.valueOf(internalRow.get(i, fields[i].dataType())));
        }
        long identifier = BatchWriteBuilder.COMMIT_IDENTIFIER;
        FileStoreCommit commit =
                ((AbstractFileStoreTable) table).store().newCommit(UUID.randomUUID().toString());
        commit.dropPartitions(Collections.singletonList(partitions), identifier);
        return true;
    }

    @Override
    public void replacePartitionMetadata(InternalRow internalRow, Map<String, String> map) throws NoSuchPartitionException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> loadPartitionMetadata(InternalRow internalRow) throws UnsupportedOperationException {
        long identifier = BatchWriteBuilder.COMMIT_IDENTIFIER;
        FileStoreCommit commit =
                ((AbstractFileStoreTable) table).store().newCommit(UUID.randomUUID().toString());
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow[] listPartitionIdentifiers(String[] partitionCols, InternalRow internalRow) {
        Preconditions.checkState(
                partitionCols.length == internalRow.numFields(),
                "Number of partition names (%s) must be equal to " +
                        "the number of partition values (%s).",
                partitionCols.length, internalRow.numFields());
        StructType schema = partitionSchema();
        Set<String> partitionColNameSet = Arrays.stream(schema.fieldNames()).collect(Collectors.toSet());
        for (String partitionCol : partitionCols) {
            Preconditions.checkState(partitionColNameSet.contains(partitionCol),
                    "Some partition names %s don't belong to " +
                            "the partition schema '%s'.", partitionCol, schema.sql());
        }
        TableScan tableScan = table.newReadBuilder().newScan();
        List<BinaryRow> binaryRows = tableScan.listPartitions();
        return binaryRows.stream().map(binaryRow -> {
            // convert binaryRow to SparkInternalRow
            SparkInternalRow sparkInternalRow = new SparkInternalRow((RowType) SparkTypeUtils.toPaimonType(schema));
            return sparkInternalRow.replace(binaryRow);
        }).filter(sparkInternalRow -> {
            // filter partitions not specified by the user
            if (partitionCols.length != 0) {
                for (int i = 0; i < partitionCols.length; i++) {
                    int schemaIndex = schema.fieldIndex(partitionCols[i]);
                    StructField apply = schema.apply(schemaIndex);
                    if (!sparkInternalRow.get(schemaIndex, apply.dataType()).equals(internalRow.get(i, apply.dataType()))) {
                        return false;
                    }
                }
            }
            return true;
        }).toArray(InternalRow[]::new);
    }
}
