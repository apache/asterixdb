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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

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
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The scan's readers share the rows an equality delete file loads through {@link IcebergDeleteCache}, so those rows
 * must stay correct when several readers use them at once, whatever the type of the delete key. Each case writes a
 * table keyed on one type, at the top level or inside a struct, deletes a subset of its keys, and checks that a
 * reader without the cache keeps exactly the rows that were not deleted, and that readers through the cache, alone
 * and concurrently, keep exactly what that reader keeps.
 */
@RunWith(Parameterized.class)
public class IcebergEqualityDeleteKeyTypesTest {

    private static final int DATA_FILES = 4;
    private static final int ROWS_PER_FILE = 200;
    private static final int KEYS = 50;
    private static final int READERS = 16;
    private static final int ROUNDS = 10;

    private final String typeName;
    private final Type keyType;
    private final IntFunction<Object> keyValue;
    private final IntPredicate deletedKey;
    private final boolean nested;
    private final int formatVersion;

    private File warehouse;
    private Schema schema;
    private Table table;
    private List<FileScanTask> tasks;
    private FileIO io;

    @Parameterized.Parameters(name = "{0}, nested={4}")
    public static Collection<Object[]> keyTypes() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean nested : new boolean[] { false, true }) {
            add(cases, "boolean", Types.BooleanType.get(), k -> k % 2 == 1, k -> k % 2 == 1, nested, 2);
            add(cases, "int", Types.IntegerType.get(), k -> k, k -> k < 20, nested, 2);
            add(cases, "long", Types.LongType.get(), k -> k * 1_000_000_007L, k -> k < 20, nested, 2);
            add(cases, "float", Types.FloatType.get(), k -> k + 0.5f, k -> k < 20, nested, 2);
            add(cases, "double", Types.DoubleType.get(), k -> k + 0.25d, k -> k < 20, nested, 2);
            add(cases, "decimal", Types.DecimalType.of(9, 2), k -> BigDecimal.valueOf(k * 100L + 7, 2), k -> k < 20,
                    nested, 2);
            add(cases, "date", Types.DateType.get(), k -> LocalDate.of(2020, 1, 1).plusDays(k), k -> k < 20, nested, 2);
            add(cases, "time", Types.TimeType.get(), k -> LocalTime.of(0, 0).plusSeconds(k * 61L), k -> k < 20, nested,
                    2);
            add(cases, "timestamp", Types.TimestampType.withoutZone(),
                    k -> LocalDateTime.of(2020, 1, 1, 0, 0).plusMinutes(k), k -> k < 20, nested, 2);
            add(cases, "timestamptz", Types.TimestampType.withZone(),
                    k -> OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).plusMinutes(k), k -> k < 20, nested,
                    2);
            add(cases, "timestamp_ns", Types.TimestampNanoType.withoutZone(),
                    k -> LocalDateTime.of(2020, 1, 1, 0, 0).plusNanos(k * 1_001L), k -> k < 20, nested, 3);
            add(cases, "timestamptz_ns", Types.TimestampNanoType.withZone(),
                    k -> OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).plusNanos(k * 1_001L), k -> k < 20,
                    nested, 3);
            add(cases, "string", Types.StringType.get(), k -> "key-" + k, k -> k < 20, nested, 2);
            add(cases, "uuid", Types.UUIDType.get(), k -> new UUID(k, k * 31L), k -> k < 20, nested, 2);
            add(cases, "fixed", Types.FixedType.ofLength(16), IcebergEqualityDeleteKeyTypesTest::fixedBytes,
                    k -> k < 20, nested, 2);
            add(cases, "binary", Types.BinaryType.get(),
                    k -> ByteBuffer.wrap(("bin-" + k).getBytes(StandardCharsets.UTF_8)), k -> k < 20, nested, 2);
        }
        return cases;
    }

    private static void add(List<Object[]> cases, String name, Type type, IntFunction<Object> value,
            IntPredicate deleted, boolean nested, int formatVersion) {
        cases.add(new Object[] { name, type, value, deleted, nested, formatVersion });
    }

    public IcebergEqualityDeleteKeyTypesTest(String typeName, Type keyType, IntFunction<Object> keyValue,
            IntPredicate deletedKey, boolean nested, int formatVersion) {
        this.typeName = typeName;
        this.keyType = keyType;
        this.keyValue = keyValue;
        this.deletedKey = deletedKey;
        this.nested = nested;
        this.formatVersion = formatVersion;
    }

    @Before
    public void createTable() throws Exception {
        warehouse = Files.createTempDirectory("iceberg-eq-key-" + typeName).toFile();
        if (nested) {
            schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "s",
                            Types.StructType.of(Types.NestedField.required(3, "key", keyType),
                                    Types.NestedField.required(4, "other", Types.IntegerType.get()))));
        } else {
            schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "key", keyType));
        }
        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        table = tables.create(schema, PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)), warehouse.toString());
        for (int f = 0; f < DATA_FILES; f++) {
            table.newAppend().appendFile(writeDataFile(f)).commit();
        }
        table.newRowDelta().addDeletes(writeEqualityDelete()).commit();
        tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        Assert.assertEquals(DATA_FILES, tasks.size());
        for (FileScanTask task : tasks) {
            Assert.assertEquals("the equality delete must apply to every data file", 1, task.deletes().size());
        }
        io = table.io();
    }

    @After
    public void deleteWarehouse() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(warehouse);
    }

    @Test
    public void withoutCache() throws Exception {
        // Iceberg reads equality delete files reusing its containers, and its fixed-length reader fills the same
        // array for every row it reads, so every loaded fixed key holds the last one read: only that key is deleted,
        // with or without the cache. The cache must still change nothing, which the other tests check.
        Assume.assumeFalse("fixed-length equality keys are mis-loaded by Iceberg itself", "fixed".equals(typeName));
        Assert.assertEquals(expectedIds(), readAll(null));
    }

    @Test
    public void throughCache() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        Set<Integer> uncached = readAll(null);
        Assert.assertEquals(uncached, readAll(cache));
        Assert.assertEquals("a reader served from the cache", uncached, readAll(cache));
    }

    @Test
    public void concurrentlyThroughCache() throws Exception {
        Set<Integer> uncached = readAll(null);
        ExecutorService pool = Executors.newFixedThreadPool(READERS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                IcebergDeleteCache cache = new IcebergDeleteCache();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Set<Integer>>> results = new ArrayList<>();
                for (int r = 0; r < READERS; r++) {
                    Callable<Set<Integer>> reader = () -> {
                        start.await();
                        return readAll(cache);
                    };
                    results.add(pool.submit(reader));
                }
                start.countDown();
                for (Future<Set<Integer>> result : results) {
                    Assert.assertEquals("round " + round, uncached, result.get());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private Set<Integer> readAll(IcebergDeleteCache cache) throws Exception {
        Set<Integer> ids = new TreeSet<>();
        for (FileScanTask task : tasks) {
            InputFile inFile = io.newInputFile(task.file().location(), task.file().fileSizeInBytes());
            try (CloseableIterable<Record> rows =
                    IcebergFileRecordReader.openStandardRead(io, inFile, task, schema, schema, cache)) {
                for (Record row : rows) {
                    ids.add((Integer) row.getField("id"));
                }
            }
        }
        return ids;
    }

    private Set<Integer> expectedIds() {
        Set<Integer> ids = new TreeSet<>();
        for (int id = 0; id < DATA_FILES * ROWS_PER_FILE; id++) {
            if (!deletedKey.test(key(id))) {
                ids.add(id);
            }
        }
        Assert.assertFalse("the case must keep some rows", ids.isEmpty());
        Assert.assertTrue("the case must delete some rows", ids.size() < DATA_FILES * ROWS_PER_FILE);
        return ids;
    }

    private static int key(int id) {
        return id % KEYS;
    }

    private static byte[] fixedBytes(int k) {
        byte[] bytes = new byte[16];
        bytes[0] = (byte) k;
        bytes[15] = (byte) (k * 7);
        return bytes;
    }

    private Record row(Schema rowSchema, int id, Object key, boolean withOther) {
        GenericRecord record = GenericRecord.create(rowSchema);
        if (withOther) {
            record.setField("id", id);
        }
        if (nested) {
            Types.StructType struct = rowSchema.findField("s").type().asStructType();
            GenericRecord inner = GenericRecord.create(struct);
            inner.setField("key", key);
            if (withOther) {
                inner.setField("other", id);
            }
            record.setField("s", inner);
        } else {
            record.setField("key", key);
        }
        return record;
    }

    private DataFile writeDataFile(int fileIndex) throws Exception {
        File file = new File(warehouse, "data/part-" + fileIndex + ".parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(schema)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (FileAppender<Record> writer = appender) {
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                int id = fileIndex * ROWS_PER_FILE + i;
                writer.add(row(schema, id, keyValue.apply(key(id)), true));
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(org.apache.iceberg.Files.localInput(file))
                .withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET).build();
    }

    private DeleteFile writeEqualityDelete() throws Exception {
        String keyPath = nested ? "s.key" : "key";
        Schema deleteSchema = schema.select(keyPath);
        File file = new File(warehouse, "data/eq-delete.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(schema.findField(keyPath).fieldId())).overwrite()
                .buildEqualityWriter();
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int k = 0; k < KEYS; k++) {
                if (deletedKey.test(k)) {
                    open.write(row(deleteSchema, -1, keyValue.apply(k), false));
                }
            }
        }
        return writer.toDeleteFile();
    }
}
