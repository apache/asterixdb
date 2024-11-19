/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.external_dataset.deltalake;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class DeltaTableGenerator {
    public static final String DELTA_GEN_BASEDIR = "target" + File.separatorChar + "generated_delta_files";
    public static final String DELTA_EMPTY_TABLE =
            "target" + File.separatorChar + "generated_delta_files" + File.separatorChar + "empty_delta_table";
    public static final String DELTA_MODIFIED_TABLE =
            "target" + File.separatorChar + "generated_delta_files" + File.separatorChar + "modified_delta_table";
    public static final String DELTA_MULTI_FILE_TABLE =
            "target" + File.separatorChar + "generated_delta_files" + File.separatorChar + "multiple_file_delta_table";

    public static void prepareDeltaTableContainer(Configuration conf) {
        File basePath = new File(".");
        cleanBinaryDirectory(basePath, DELTA_GEN_BASEDIR);
        prepareMultipleFilesTable(conf);
        prepareModifiedTable(conf);
        prepareEmptyTable(conf);
    }

    public static void cleanBinaryDirectory(File localDataRoot, String binaryFilesPath) {
        try {
            File destPath = new File(localDataRoot, binaryFilesPath);
            //Delete old generated files
            if (destPath.exists()) {
                IoUtil.delete(destPath);
            }
            //Create new directory
            Files.createDirectory(Paths.get(destPath.getAbsolutePath()));
        } catch (IOException e) {

        }

    }

    public static void prepareEmptyTable(Configuration conf) {
        List<Action> actions = List.of();
        DeltaLog log = DeltaLog.forTable(conf, DELTA_EMPTY_TABLE);
        OptimisticTransaction txn = log.startTransaction();
        Metadata metaData = txn.metadata().copyBuilder().partitionColumns(new ArrayList<>())
                .schema(new StructType().add(new StructField("id", new IntegerType(), true))
                        .add(new StructField("data", new StringType(), true)))
                .build();
        txn.updateMetadata(metaData);
        txn.commit(actions, new Operation(Operation.Name.CREATE_TABLE), "deltalake-table-create");
    }

    public static void prepareModifiedTable(Configuration conf) {
        Schema schema = SchemaBuilder.record("MyRecord").fields().requiredInt("id").requiredString("data").endRecord();
        try {
            Path path = new Path(DELTA_MODIFIED_TABLE, "firstFile.parquet");
            ParquetWriter<GenericData.Record> writer =
                    AvroParquetWriter.<GenericData.Record> builder(path).withConf(conf).withSchema(schema).build();

            List<GenericData.Record> fileFirstSnapshotRecords = List.of(new GenericData.Record(schema),
                    new GenericData.Record(schema), new GenericData.Record(schema));
            List<GenericData.Record> fileSecondSnapshotRecords = List.of(new GenericData.Record(schema));

            fileFirstSnapshotRecords.get(0).put("id", 0);
            fileFirstSnapshotRecords.get(0).put("data", "vibrant_mclean");

            fileFirstSnapshotRecords.get(1).put("id", 1);
            fileFirstSnapshotRecords.get(1).put("data", "frosty_wilson");

            fileFirstSnapshotRecords.get(2).put("id", 2);
            fileFirstSnapshotRecords.get(2).put("data", "serene_kirby");

            fileSecondSnapshotRecords.get(0).put("id", 2);
            fileSecondSnapshotRecords.get(0).put("data", "serene_kirby");

            for (GenericData.Record record : fileFirstSnapshotRecords) {
                writer.write(record);
            }

            long size = writer.getDataSize();
            writer.close();

            List<Action> actions = List.of(new AddFile("firstFile.parquet", new HashMap<>(), size,
                    System.currentTimeMillis(), true, null, null));
            DeltaLog log = DeltaLog.forTable(conf, DELTA_MODIFIED_TABLE);
            OptimisticTransaction txn = log.startTransaction();
            Metadata metaData = txn.metadata().copyBuilder().partitionColumns(new ArrayList<>())
                    .schema(new StructType().add(new StructField("id", new IntegerType(), true))
                            .add(new StructField("data", new StringType(), true)))
                    .build();
            txn.updateMetadata(metaData);
            txn.commit(actions, new Operation(Operation.Name.CREATE_TABLE), "deltalake-table-create");

            Path path2 = new Path(DELTA_MODIFIED_TABLE, "secondFile.parquet");
            ParquetWriter<GenericData.Record> writer2 =
                    AvroParquetWriter.<GenericData.Record> builder(path2).withConf(conf).withSchema(schema).build();

            for (GenericData.Record record : fileSecondSnapshotRecords) {
                writer2.write(record);
            }
            long size2 = writer2.getDataSize();
            writer2.close();
            AddFile addFile = new AddFile("firstFile.parquet", new HashMap<>(), size, System.currentTimeMillis(), true,
                    null, null);
            RemoveFile removeFile = addFile.remove();
            List<Action> actions3 = List.of(removeFile, new AddFile("secondFile.parquet", new HashMap<>(), size2,
                    System.currentTimeMillis(), true, null, null));
            OptimisticTransaction txn3 = log.startTransaction();
            txn3.commit(actions3, new Operation(Operation.Name.DELETE), "deltalake-table-delete");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void prepareMultipleFilesTable(Configuration conf) {
        Schema schema = SchemaBuilder.record("MyRecord").fields().requiredInt("id").requiredString("name")
                .requiredString("age").endRecord();
        try {
            Path path = new Path(DELTA_MULTI_FILE_TABLE, "firstFile.parquet");
            ParquetWriter<GenericData.Record> writer =
                    AvroParquetWriter.<GenericData.Record> builder(path).withConf(conf).withSchema(schema).build();

            List<GenericData.Record> fileFirstSnapshotRecords = List.of(new GenericData.Record(schema),
                    new GenericData.Record(schema), new GenericData.Record(schema));
            List<GenericData.Record> fileSecondSnapshotRecords = List.of(new GenericData.Record(schema));

            fileFirstSnapshotRecords.get(0).put("id", 0);
            fileFirstSnapshotRecords.get(0).put("name", "Cooper");
            fileFirstSnapshotRecords.get(0).put("age", "42");

            fileFirstSnapshotRecords.get(1).put("id", 1);
            fileFirstSnapshotRecords.get(1).put("name", "Murphy");
            fileFirstSnapshotRecords.get(1).put("age", "16");

            fileFirstSnapshotRecords.get(2).put("id", 2);
            fileFirstSnapshotRecords.get(2).put("name", "Mann");
            fileFirstSnapshotRecords.get(2).put("age", "45");

            fileSecondSnapshotRecords.get(0).put("id", 3);
            fileSecondSnapshotRecords.get(0).put("name", "Brand");
            fileSecondSnapshotRecords.get(0).put("age", "35");

            for (GenericData.Record record : fileFirstSnapshotRecords) {
                writer.write(record);
            }

            long size = writer.getDataSize();
            writer.close();

            List<Action> actions = List.of(new AddFile("firstFile.parquet", new HashMap<>(), size,
                    System.currentTimeMillis(), true, null, null));
            DeltaLog log = DeltaLog.forTable(conf, DELTA_MULTI_FILE_TABLE);
            OptimisticTransaction txn = log.startTransaction();
            Metadata metaData = txn.metadata().copyBuilder().partitionColumns(new ArrayList<>())
                    .schema(new StructType().add(new StructField("id", new IntegerType(), true))
                            .add(new StructField("name", new StringType(), true))
                            .add(new StructField("age", new StringType(), true)))
                    .build();
            txn.updateMetadata(metaData);
            txn.commit(actions, new Operation(Operation.Name.CREATE_TABLE), "deltalake-table-create");

            Path path2 = new Path(DELTA_MULTI_FILE_TABLE, "secondFile.parquet");
            ParquetWriter<GenericData.Record> writer2 =
                    AvroParquetWriter.<GenericData.Record> builder(path2).withConf(conf).withSchema(schema).build();

            for (GenericData.Record record : fileSecondSnapshotRecords) {
                writer2.write(record);
            }

            long size2 = writer2.getDataSize();
            writer2.close();

            List<Action> actions2 = List.of(new AddFile("secondFile.parquet", new HashMap<>(), size2,
                    System.currentTimeMillis(), true, null, null));
            OptimisticTransaction txn2 = log.startTransaction();
            txn2.commit(actions2, new Operation(Operation.Name.WRITE), "deltalake-table-create");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
