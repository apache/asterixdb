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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Equality deletes keyed on a {@code fixed} column, loaded by {@link FixedKeySafeDeleteLoader}: every deleted key is
 * kept, where Iceberg's own loader keeps only the last one read. The key-type matrix in
 * {@code IcebergEqualityDeleteKeyTypesTest} covers the standard read with and without the cache; this covers the
 * loader on its own and the delete filter of the variant-pruned read.
 */
public class FixedKeySafeDeleteLoaderTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "k", Types.FixedType.ofLength(16)));
    private static final Schema KEY = SCHEMA.select("k");
    private static final int ROWS = 200;
    private static final int KEYS = 50;
    private static final int DELETED_KEYS = 20;

    private File warehouse;
    private Table table;
    private FileScanTask task;
    private FileIO io;

    @Before
    public void createTable() throws Exception {
        warehouse = Files.createTempDirectory("fixed-key-deletes").toFile();
        table = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(SCHEMA,
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"), warehouse.toString());
        table.newAppend().appendFile(writeDataFile()).commit();
        table.newRowDelta().addDeletes(writeEqualityDelete()).commit();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            task = planned.iterator().next();
        }
        Assert.assertEquals("the equality delete must apply to the data file", 1, task.deletes().size());
        io = table.io();
    }

    @After
    public void deleteWarehouse() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(warehouse);
    }

    @Test
    public void everyFixedKeyIsLoaded() {
        StructLikeSet deleted = new FixedKeySafeDeleteLoader(this::open).loadEqualityDeletes(task.deletes(), KEY);
        Assert.assertEquals(DELETED_KEYS, deleted.size());
    }

    /** Readers of a scan sharing the delete cache load a fixed-keyed delete file once, as they do any other. */
    @Test
    public void aFixedKeyedFileIsLoadedOnceThroughTheCache() {
        AtomicInteger opens = new AtomicInteger();
        IcebergDeleteCache cache = new IcebergDeleteCache();
        for (int reader = 0; reader < 2; reader++) {
            StructLikeSet deleted = cache.newLoader(file -> {
                opens.incrementAndGet();
                return open(file);
            }).loadEqualityDeletes(task.deletes(), KEY);
            Assert.assertEquals(DELETED_KEYS, deleted.size());
        }
        Assert.assertEquals("the second reader must be served from the cache", 1, opens.get());
        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void withoutTheCacheEveryLoadReadsTheFile() {
        AtomicInteger opens = new AtomicInteger();
        for (int reader = 0; reader < 2; reader++) {
            StructLikeSet deleted = new FixedKeySafeDeleteLoader(file -> {
                opens.incrementAndGet();
                return open(file);
            }).loadEqualityDeletes(task.deletes(), KEY);
            Assert.assertEquals(DELETED_KEYS, deleted.size());
        }
        Assert.assertEquals(2, opens.get());
    }

    /**
     * Tripwire: Iceberg's own loader keeps one key out of {@link #DELETED_KEYS}. When this fails, Iceberg has fixed
     * the defect and {@link FixedKeySafeDeleteLoader} can be retired.
     */
    @Test
    public void icebergsOwnLoaderStillKeepsOnlyTheLastKey() {
        StructLikeSet deleted = new BaseDeleteLoader(this::open).loadEqualityDeletes(task.deletes(), KEY);
        Assert.assertEquals(1, deleted.size());
    }

    @Test
    public void theVariantPrunedReadsDeleteFilterAppliesEveryFixedKey() throws Exception {
        PositionlessGenericDeleteFilter filter = new PositionlessGenericDeleteFilter(io, task, SCHEMA, SCHEMA);
        Predicate<Record> keep = filter.eqDeletedRowFilter();
        Schema required = filter.requiredSchema();
        Set<Integer> kept = new TreeSet<>();
        try (CloseableIterable<Record> rows = Parquet.read(io.newInputFile(task.file().location())).project(required)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(required, fileSchema)).build()) {
            for (Record row : rows) {
                if (keep.test(row)) {
                    kept.add((Integer) row.getField("id"));
                }
            }
        }
        Assert.assertEquals(expectedIds(), kept);
    }

    @Test
    public void aFixedFieldIsFoundAtAnyDepth() {
        Assert.assertTrue(FixedKeySafeDeleteLoader.hasFixedField(KEY));
        Assert.assertTrue(FixedKeySafeDeleteLoader.hasFixedField(new Schema(Types.NestedField.optional(3, "st",
                Types.StructType.of(Types.NestedField.optional(4, "k", Types.FixedType.ofLength(4)))))));
        Assert.assertFalse(FixedKeySafeDeleteLoader.hasFixedField(SCHEMA.select("id")));
    }

    private org.apache.iceberg.io.InputFile open(DeleteFile deleteFile) {
        return io.newInputFile(deleteFile.location(), deleteFile.fileSizeInBytes());
    }

    private static Set<Integer> expectedIds() {
        Set<Integer> ids = new TreeSet<>();
        for (int id = 0; id < ROWS; id++) {
            if (id % KEYS >= DELETED_KEYS) {
                ids.add(id);
            }
        }
        return ids;
    }

    private static byte[] key(int k) {
        byte[] bytes = new byte[16];
        bytes[0] = (byte) k;
        bytes[15] = (byte) (k * 7);
        return bytes;
    }

    private DataFile writeDataFile() throws Exception {
        File file = new File(warehouse, "data/data.parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (FileAppender<Record> writer = appender) {
            for (int id = 0; id < ROWS; id++) {
                Record row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                row.setField("k", key(id % KEYS));
                writer.add(row);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(org.apache.iceberg.Files.localInput(file))
                .withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET).build();
    }

    private DeleteFile writeEqualityDelete() throws Exception {
        File file = new File(warehouse, "data/eq-delete.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(KEY).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(List.of(2)).overwrite().buildEqualityWriter();
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int k = 0; k < DELETED_KEYS; k++) {
                Record row = GenericRecord.create(KEY);
                row.setField("k", key(k));
                open.write(row);
            }
        }
        return writer.toDeleteFile();
    }
}
