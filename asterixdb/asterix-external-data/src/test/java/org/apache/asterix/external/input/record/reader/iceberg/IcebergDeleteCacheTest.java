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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An equality delete applies to every data file written before it in its partition. These tests pin down that a scan's
 * readers load such a delete file once, shared through {@link IcebergDeleteCache}, that they open it with the length
 * its manifest records, and that the rows it deletes stay deleted.
 */
public class IcebergDeleteCacheTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "bucket", Types.IntegerType.get()));
    private static final int DATA_FILES = 3;
    private static final int ROWS_PER_FILE = 10;
    private static final Set<Integer> DELETED_BUCKETS = Set.of(1, 2);

    private File warehouse;
    private Table table;
    private DeleteFile deleteFile;
    private List<FileScanTask> tasks;
    private CountingFileIO io;

    @Before
    public void createTable() throws Exception {
        warehouse = Files.createTempDirectory("iceberg-delete-cache").toFile();
        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        table = tables.create(SCHEMA, PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"),
                warehouse.toString());
        for (int f = 0; f < DATA_FILES; f++) {
            table.newAppend().appendFile(writeDataFile(f)).commit();
        }
        deleteFile = writeEqualityDelete();
        table.newRowDelta().addDeletes(deleteFile).commit();
        tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        Assert.assertEquals(DATA_FILES, tasks.size());
        for (FileScanTask task : tasks) {
            Assert.assertEquals("the equality delete must apply to every data file", 1, task.deletes().size());
        }
        io = new CountingFileIO(table.io());
    }

    @After
    public void deleteWarehouse() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(warehouse);
    }

    @Test
    public void withoutCacheEveryDataFileReloadsTheDeleteFile() throws Exception {
        Assert.assertEquals(expectedIds(), readAll(null));
        Assert.assertEquals(DATA_FILES, io.opens(deleteFile.location()));
    }

    @Test
    public void withCacheTheDeleteFileIsLoadedOnce() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals("one load must serve every data file", 1, io.opens(deleteFile.location()));
        Assert.assertEquals(0, io.opensWithoutLength(deleteFile.location()));
    }

    /** Iceberg checks the budget before it looks in the cache, so a full cache must still serve what it holds. */
    @Test
    public void aFullCacheStillServesTheFilesItHolds() throws Exception {
        long fileSize = sizeOfOneCachedFile();
        IcebergDeleteCache cache = new IcebergDeleteCache(IcebergDeleteCache.MAX_ENTRY_SIZE, fileSize);
        io = new CountingFileIO(table.io());
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals("the budget is exhausted by this one file", fileSize, cache.cachedSize());
        Assert.assertEquals("every data file must be served from the one load", 1, io.opens(deleteFile.location()));
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals("a later scan of the same file must be served too", 1, io.opens(deleteFile.location()));
    }

    @Test
    public void aFileBeyondTheBudgetIsLoadedWithoutBeingKept() throws Exception {
        long fileSize = sizeOfOneCachedFile();
        IcebergDeleteCache cache = new IcebergDeleteCache(IcebergDeleteCache.MAX_ENTRY_SIZE, fileSize - 1);
        io = new CountingFileIO(table.io());
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals(DATA_FILES, io.opens(deleteFile.location()));
        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(0, cache.cachedSize());
    }

    /** Partitions on a node read concurrently through one cache; they must not load the file once each. */
    @Test
    public void concurrentReadersShareOneLoad() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        int readers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        try {
            List<Future<Set<Integer>>> results = new ArrayList<>();
            for (int r = 0; r < readers; r++) {
                results.add(pool.submit(() -> readAll(cache)));
            }
            for (Future<Set<Integer>> result : results) {
                Assert.assertEquals(expectedIds(), result.get());
            }
        } finally {
            pool.shutdownNow();
        }
        Assert.assertEquals(1, io.opens(deleteFile.location()));
    }

    /** A load that fails must not be remembered: the reader sees the failure, and the next reader loads again. */
    @Test
    public void failedLoadIsNotKeptAndTheNextReaderRetriesIt() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        io.failNextOpens(deleteFile.location(), 1);
        try {
            readAll(cache);
            Assert.fail("the failed delete load must surface, not be swallowed");
        } catch (RuntimeException expected) {
            // the injected failure
        }
        Assert.assertEquals("a failed load must not stay in the cache", 0, cache.size());
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals("one failed attempt, then one load that serves every data file", 2,
                io.opens(deleteFile.location()));
    }

    /**
     * Readers waiting on a load that fails share its failure; none of them may go on without the deletes and return
     * rows that should have been deleted, and once the failure is gone the cache serves the right rows.
     */
    @Test
    public void readersRacingOnAFailedLoadNeverReturnDeletedRows() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        io.failNextOpens(deleteFile.location(), 1);
        int readers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        int failures = 0;
        try {
            List<Future<Set<Integer>>> results = new ArrayList<>();
            for (int r = 0; r < readers; r++) {
                results.add(pool.submit(() -> readAll(cache)));
            }
            for (Future<Set<Integer>> result : results) {
                try {
                    Assert.assertEquals("a reader that succeeds must return exactly the live rows", expectedIds(),
                            result.get());
                } catch (java.util.concurrent.ExecutionException e) {
                    failures++;
                }
            }
        } finally {
            pool.shutdownNow();
        }
        Assert.assertTrue("the injected failure must reach at least one reader", failures >= 1);
        Assert.assertEquals(expectedIds(), readAll(cache));
    }

    /** The size the cache charges for the test's delete file, read off a cache that has loaded it. */
    private long sizeOfOneCachedFile() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache();
        Assert.assertEquals(expectedIds(), readAll(cache));
        Assert.assertEquals(1, cache.size());
        Assert.assertTrue(cache.cachedSize() > 1);
        return cache.cachedSize();
    }

    private Set<Integer> readAll(IcebergDeleteCache cache) throws Exception {
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

    private static Set<Integer> expectedIds() {
        Set<Integer> ids = new TreeSet<>();
        for (int id = 0; id < DATA_FILES * ROWS_PER_FILE; id++) {
            if (!DELETED_BUCKETS.contains(bucket(id))) {
                ids.add(id);
            }
        }
        return ids;
    }

    private static int bucket(int id) {
        return id % 5;
    }

    private DataFile writeDataFile(int fileIndex) throws Exception {
        File file = new File(warehouse, "data/part-" + fileIndex + ".parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).build();
        GenericRecord template = GenericRecord.create(SCHEMA);
        try (FileAppender<Record> writer = appender) {
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                int id = fileIndex * ROWS_PER_FILE + i;
                Record record = template.copy();
                record.setField("id", id);
                record.setField("bucket", bucket(id));
                writer.add(record);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(org.apache.iceberg.Files.localInput(file))
                .withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET).build();
    }

    private DeleteFile writeEqualityDelete() throws Exception {
        Schema deleteSchema = SCHEMA.select("bucket");
        File file = new File(warehouse, "data/eq-delete.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(SCHEMA.findField("bucket").fieldId())).overwrite()
                .buildEqualityWriter();
        GenericRecord template = GenericRecord.create(deleteSchema);
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int bucket : DELETED_BUCKETS) {
                Record row = template.copy();
                row.setField("bucket", bucket);
                open.write(row);
            }
        }
        return writer.toDeleteFile();
    }

    /** Counts the files opened for reading, and whether they were opened without their length. */
    private static final class CountingFileIO implements FileIO {
        private final FileIO delegate;
        private final Map<String, AtomicInteger> opens = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> opensWithoutLength = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();

        CountingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        /** The next {@code count} opens of {@code location} yield a file whose stream cannot be opened. */
        void failNextOpens(String location, int count) {
            failuresLeft.put(location, new AtomicInteger(count));
        }

        private InputFile maybeFailing(String path, InputFile file) {
            AtomicInteger left = failuresLeft.get(path);
            if (left == null || left.getAndUpdate(n -> Math.max(0, n - 1)) <= 0) {
                return file;
            }
            return new InputFile() {
                @Override
                public long getLength() {
                    return file.getLength();
                }

                @Override
                public org.apache.iceberg.io.SeekableInputStream newStream() {
                    throw new org.apache.iceberg.exceptions.RuntimeIOException("injected failure opening %s", path);
                }

                @Override
                public String location() {
                    return file.location();
                }

                @Override
                public boolean exists() {
                    return file.exists();
                }
            };
        }

        int opens(String location) {
            return opens.getOrDefault(location, new AtomicInteger()).get();
        }

        int opensWithoutLength(String location) {
            return opensWithoutLength.getOrDefault(location, new AtomicInteger()).get();
        }

        @Override
        public InputFile newInputFile(String path) {
            opens.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
            opensWithoutLength.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
            return maybeFailing(path, delegate.newInputFile(path));
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            opens.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
            return maybeFailing(path, delegate.newInputFile(path, length));
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }

        @Override
        public Map<String, String> properties() {
            return new HashMap<>(delegate.properties());
        }
    }
}
