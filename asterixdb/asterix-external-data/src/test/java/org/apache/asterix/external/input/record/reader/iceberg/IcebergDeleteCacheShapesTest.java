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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The delete shapes {@link IcebergEqualityDeleteKeyTypesTest} does not cover, read by readers sharing one
 * {@link IcebergDeleteCache}: position deletes, whose cached value is an index per data file; an equality delete on
 * two columns at once; and an equality delete on a nullable column that deletes the rows where it is null. Each is
 * read without the cache, through it, and through it concurrently, and must keep exactly the rows not deleted.
 */
public class IcebergDeleteCacheShapesTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "a", Types.IntegerType.get()),
            Types.NestedField.required(3, "b", Types.StringType.get()),
            Types.NestedField.optional(4, "n", Types.IntegerType.get()));
    private static final int DATA_FILES = 4;
    private static final int ROWS_PER_FILE = 200;
    private static final int READERS = 16;
    private static final int ROUNDS = 10;

    private File warehouse;
    private Table table;
    private List<DataFile> dataFiles;

    @Before
    public void createTable() throws Exception {
        warehouse = Files.createTempDirectory("iceberg-delete-shapes").toFile();
        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        table = tables.create(SCHEMA, PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"),
                warehouse.toString());
        dataFiles = new ArrayList<>();
        for (int f = 0; f < DATA_FILES; f++) {
            DataFile file = writeDataFile(f);
            dataFiles.add(file);
            table.newAppend().appendFile(file).commit();
        }
    }

    @After
    public void deleteWarehouse() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(warehouse);
    }

    /** One position delete file removing rows from every data file: the cache holds one index per data file. */
    @Test
    public void positionDeletesAcrossDataFiles() throws Exception {
        Set<Integer> deleted = new TreeSet<>();
        File file = new File(warehouse, "data/pos-delete.parquet");
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .withSpec(table.spec()).overwrite().buildPositionWriter();
        try (PositionDeleteWriter<Record> open = writer) {
            PositionDelete<Record> delete = PositionDelete.create();
            List<DataFile> byPath = new ArrayList<>(dataFiles);
            byPath.sort((x, y) -> x.location().compareTo(y.location()));
            for (DataFile dataFile : byPath) {
                int fileIndex = dataFiles.indexOf(dataFile);
                for (int pos = 0; pos < ROWS_PER_FILE; pos++) {
                    if (pos % 7 == fileIndex) {
                        open.write(delete.set(dataFile.location(), pos));
                        deleted.add(fileIndex * ROWS_PER_FILE + pos);
                    }
                }
            }
        }
        commit(writer.toDeleteFile());
        assertEveryReadKeeps(expectedWithout(deleted));
    }

    /** An equality delete on (a, b): a row is deleted only when both columns match the same delete row. */
    @Test
    public void equalityDeletesOnTwoColumns() throws Exception {
        Schema deleteSchema = SCHEMA.select("a", "b");
        Set<Integer> deleted = new TreeSet<>();
        File file = new File(warehouse, "data/eq-two-columns.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Arrays.asList(2, 3)).overwrite().buildEqualityWriter();
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int a = 0; a < 10; a++) {
                // each delete row names one a with a b only some rows of that a carry
                GenericRecord row = GenericRecord.create(deleteSchema);
                row.setField("a", a);
                row.setField("b", b(a));
                open.write(row);
            }
        }
        for (int id = 0; id < DATA_FILES * ROWS_PER_FILE; id++) {
            if (a(id) < 10 && bOfRow(id).equals(b(a(id)))) {
                deleted.add(id);
            }
        }
        commit(writer.toDeleteFile());
        assertEveryReadKeeps(expectedWithout(deleted));
    }

    /** An equality delete on a nullable column whose delete row is null: it removes exactly the rows holding null. */
    @Test
    public void equalityDeletesMatchingNull() throws Exception {
        Schema deleteSchema = SCHEMA.select("n");
        Set<Integer> deleted = new TreeSet<>();
        File file = new File(warehouse, "data/eq-null.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(List.of(4)).overwrite().buildEqualityWriter();
        try (EqualityDeleteWriter<Record> open = writer) {
            GenericRecord nullRow = GenericRecord.create(deleteSchema);
            nullRow.setField("n", null);
            open.write(nullRow);
            GenericRecord three = GenericRecord.create(deleteSchema);
            three.setField("n", 3);
            open.write(three);
        }
        for (int id = 0; id < DATA_FILES * ROWS_PER_FILE; id++) {
            Integer n = n(id);
            if (n == null || n == 3) {
                deleted.add(id);
            }
        }
        commit(writer.toDeleteFile());
        assertEveryReadKeeps(expectedWithout(deleted));
    }

    private void commit(DeleteFile deleteFile) {
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    private void assertEveryReadKeeps(Set<Integer> expected) throws Exception {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        Assert.assertEquals(DATA_FILES, tasks.size());
        for (FileScanTask task : tasks) {
            Assert.assertEquals("the delete file must apply to every data file", 1, task.deletes().size());
        }
        FileIO io = table.io();
        Assert.assertEquals("without the cache", expected, readAll(io, tasks, null));
        IcebergDeleteCache shared = new IcebergDeleteCache();
        Assert.assertEquals("through the cache", expected, readAll(io, tasks, shared));
        Assert.assertEquals("served from the cache", expected, readAll(io, tasks, shared));
        ExecutorService pool = Executors.newFixedThreadPool(READERS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                IcebergDeleteCache cache = new IcebergDeleteCache();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Set<Integer>>> results = new ArrayList<>();
                for (int r = 0; r < READERS; r++) {
                    Callable<Set<Integer>> reader = () -> {
                        start.await();
                        return readAll(io, tasks, cache);
                    };
                    results.add(pool.submit(reader));
                }
                start.countDown();
                for (Future<Set<Integer>> result : results) {
                    Assert.assertEquals("concurrently, round " + round, expected, result.get());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static Set<Integer> readAll(FileIO io, List<FileScanTask> tasks, IcebergDeleteCache cache)
            throws Exception {
        Set<Integer> ids = new TreeSet<>();
        for (FileScanTask task : tasks) {
            InputFile inFile = io.newInputFile(task.file().location(), task.file().fileSizeInBytes());
            try (CloseableIterable<Record> rows =
                    IcebergFileRecordReader.openStandardRead(io, inFile, task, SCHEMA, SCHEMA, cache)) {
                for (Record row : rows) {
                    ids.add((Integer) row.getField("id"));
                }
            }
        }
        return ids;
    }

    private static Set<Integer> expectedWithout(Set<Integer> deleted) {
        Set<Integer> ids = new TreeSet<>();
        for (int id = 0; id < DATA_FILES * ROWS_PER_FILE; id++) {
            if (!deleted.contains(id)) {
                ids.add(id);
            }
        }
        Assert.assertFalse("the case must delete some rows", deleted.isEmpty());
        Assert.assertTrue("the case must keep some rows", ids.size() > 0);
        return ids;
    }

    private static int a(int id) {
        return id % 20;
    }

    private static String b(int a) {
        return "b-" + (a % 3);
    }

    private static String bOfRow(int id) {
        return "b-" + (id / 20 % 3);
    }

    private static Integer n(int id) {
        return id % 5 == 0 ? null : id % 9;
    }

    private DataFile writeDataFile(int fileIndex) throws Exception {
        File file = new File(warehouse, "data/part-" + fileIndex + ".parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (FileAppender<Record> writer = appender) {
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                int id = fileIndex * ROWS_PER_FILE + i;
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("id", id);
                record.setField("a", a(id));
                record.setField("b", bOfRow(id));
                record.setField("n", n(id));
                writer.add(record);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(org.apache.iceberg.Files.localInput(file))
                .withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET).build();
    }
}
