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
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
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
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Data and delete files are opened with the length their manifest records rather than asking storage for it, so a
 * manifest whose recorded length is wrong must never produce wrong rows: the read either fails or returns exactly the
 * right rows. Each case records a length shorter and longer than the real one, for a data file and for a delete file,
 * small enough for Iceberg to read whole and large enough to be read by range, through a FileIO that checks lengths
 * and through one that takes them on trust as object store clients do.
 */
public class IcebergWrongFileLengthTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
            Types.NestedField.optional(3, "pad", Types.StringType.get()));
    private static final int ROWS = 200;
    // Iceberg reads a Parquet file of up to 1 MiB whole and checks its length while doing so; a larger one is read by
    // range, where only Parquet's footer check can catch a wrong length. The large cases are made bigger than this.
    private static final long EAGER_FETCH_LIMIT = 1024 * 1024;
    private static final int LARGE_ROWS = 3000;
    private static final int PAD_BYTES = 512;

    private File warehouse;
    private Table table;

    @Before
    public void createTable() throws Exception {
        warehouse = Files.createTempDirectory("iceberg-wrong-length").toFile();
        table = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(SCHEMA,
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"), warehouse.toString());
    }

    @After
    public void deleteWarehouse() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(warehouse);
    }

    @Test
    public void dataFileRecordedShorterNeverReturnsWrongRows() throws Exception {
        assertDataFileWithRecordedLengthNeverReturnsWrongRows(-16);
    }

    @Test
    public void dataFileRecordedLongerNeverReturnsWrongRows() throws Exception {
        assertDataFileWithRecordedLengthNeverReturnsWrongRows(+16);
    }

    @Test
    public void deleteFileRecordedShorterNeverReturnsWrongRows() throws Exception {
        assertDeleteFileWithRecordedLengthNeverReturnsWrongRows(-16);
    }

    @Test
    public void deleteFileRecordedLongerNeverReturnsWrongRows() throws Exception {
        assertDeleteFileWithRecordedLengthNeverReturnsWrongRows(+16);
    }

    @Test
    public void largeDataFileRecordedShorterNeverReturnsWrongRows() throws Exception {
        assertLargeDataFileWithRecordedLengthNeverReturnsWrongRows(-16);
    }

    @Test
    public void largeDataFileRecordedLongerNeverReturnsWrongRows() throws Exception {
        assertLargeDataFileWithRecordedLengthNeverReturnsWrongRows(+16);
    }

    @Test
    public void largeDeleteFileRecordedShorterNeverReturnsWrongRows() throws Exception {
        assertLargeDeleteFileWithRecordedLengthNeverReturnsWrongRows(-16);
    }

    @Test
    public void largeDeleteFileRecordedLongerNeverReturnsWrongRows() throws Exception {
        assertLargeDeleteFileWithRecordedLengthNeverReturnsWrongRows(+16);
    }

    /**
     * The fallback: with useManifestFileSizes off, every case above reads exactly the right rows, through either kind of
     * FileIO, because the recorded length is never used.
     */
    @Test
    public void manifestFileSizesOffReadsEveryCaseCorrectly() throws Exception {
        Map<String, String> off = Map.of(ExternalDataConstants.IcebergOptions.USE_MANIFEST_FILE_SIZES, "false");
        for (long skew : new long[] { -16, +16 }) {
            for (int shape = 0; shape < 4; shape++) {
                deleteWarehouse();
                createTable();
                List<Integer> correct;
                switch (shape) {
                    case 0:
                        table.newAppend().appendFile(writeDataFile(skew)).commit();
                        correct = ids(ROWS, false);
                        break;
                    case 1:
                        table.newAppend().appendFile(writeDataFile(0)).commit();
                        table.newRowDelta().addDeletes(writeDelete(skew)).commit();
                        correct = ids(ROWS, true);
                        break;
                    case 2:
                        table.newAppend().appendFile(writeDataFile(skew, LARGE_ROWS, PAD_BYTES)).commit();
                        correct = ids(LARGE_ROWS, false);
                        break;
                    default:
                        table.newAppend().appendFile(writeDataFile(0, ROWS, PAD_BYTES)).commit();
                        table.newRowDelta().addDeletes(writeLargeDelete(skew)).commit();
                        correct = ids(ROWS, false);
                        break;
                }
                for (FileIO io : new FileIO[] { table.io(), new TrustingLengthFileIO(table.io()) }) {
                    FileIO readIo = IcebergFileRecordReader.readFileIo(io, off);
                    for (IcebergDeleteCache cache : new IcebergDeleteCache[] { null, new IcebergDeleteCache() }) {
                        List<Integer> rows = readAll(readIo, cache);
                        java.util.Collections.sort(rows);
                        Assert.assertEquals("shape " + shape + ", skew " + skew + ", " + io.getClass().getSimpleName(),
                                correct, rows);
                    }
                }
            }
        }
    }

    @Test
    public void manifestFileSizesAreUsedUnlessTheCollectionTurnsThemOff() {
        Assert.assertTrue(IcebergFileRecordReader.useManifestFileSizes(Map.of()));
        Assert.assertTrue(IcebergFileRecordReader
                .useManifestFileSizes(Map.of(ExternalDataConstants.IcebergOptions.USE_MANIFEST_FILE_SIZES, "TRUE")));
        Assert.assertFalse(IcebergFileRecordReader
                .useManifestFileSizes(Map.of(ExternalDataConstants.IcebergOptions.USE_MANIFEST_FILE_SIZES, "false")));
        Assert.assertSame(table.io(), IcebergFileRecordReader.readFileIo(table.io(), Map.of()));
    }

    /** Anything but a boolean is rejected when the collection is created, rather than silently reading as false. */
    @Test
    public void aValueThatIsNotABooleanIsRejected() throws Exception {
        try {
            org.apache.asterix.external.util.iceberg.IcebergUtils.validateIcebergTableProperties(Map.of(
                    org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY, "tbl",
                    org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY, "ns",
                    ExternalDataConstants.IcebergOptions.USE_MANIFEST_FILE_SIZES, "ture"));
            Assert.fail("a misspelt boolean must be rejected");
        } catch (org.apache.asterix.common.exceptions.CompilationException e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains(ExternalDataConstants.IcebergOptions.USE_MANIFEST_FILE_SIZES));
        }
    }

    /** The control: the true length reads every row, so the failures above are the wrong length and nothing else. */
    @Test
    public void trueLengthsReadEveryRowNotDeleted() throws Exception {
        DataFile data = writeDataFile(0);
        table.newAppend().appendFile(data).commit();
        table.newRowDelta().addDeletes(writeDelete(0)).commit();
        for (FileIO io : new FileIO[] { table.io(), new TrustingLengthFileIO(table.io()) }) {
            Assert.assertEquals(ROWS - ROWS / 5, readAll(io, new IcebergDeleteCache()).size());
            Assert.assertEquals(ROWS - ROWS / 5, readAll(io, null).size());
        }
    }

    private void assertLargeDataFileWithRecordedLengthNeverReturnsWrongRows(long skew) throws Exception {
        DataFile data = writeDataFile(skew, LARGE_ROWS, PAD_BYTES);
        Assert.assertTrue("must be read by range, not whole", data.fileSizeInBytes() > EAGER_FETCH_LIMIT + 16);
        table.newAppend().appendFile(data).commit();
        assertNeverWrongRows(ids(LARGE_ROWS, false), true);
    }

    private void assertLargeDeleteFileWithRecordedLengthNeverReturnsWrongRows(long skew) throws Exception {
        // padded rows, so the delete file's value ranges overlap the data file's and planning attaches it
        table.newAppend().appendFile(writeDataFile(0, ROWS, PAD_BYTES)).commit();
        DeleteFile delete = writeLargeDelete(skew);
        Assert.assertTrue("must be read by range, not whole", delete.fileSizeInBytes() > EAGER_FETCH_LIMIT + 16);
        Assert.assertEquals(Collections.singletonList(3), delete.equalityFieldIds());
        table.newRowDelta().addDeletes(delete).commit();
        assertNeverWrongRows(ids(ROWS, false), true);
    }

    private void assertDataFileWithRecordedLengthNeverReturnsWrongRows(long skew) throws Exception {
        DataFile data = writeDataFile(skew);
        table.newAppend().appendFile(data).commit();
        assertNeverWrongRows(ids(ROWS, false), false);
    }

    private void assertDeleteFileWithRecordedLengthNeverReturnsWrongRows(long skew) throws Exception {
        table.newAppend().appendFile(writeDataFile(0)).commit();
        DeleteFile delete = writeDelete(skew);
        Assert.assertEquals("the equality ids must survive, or planning fails before any file is read",
                Collections.singletonList(2), delete.equalityFieldIds());
        table.newRowDelta().addDeletes(delete).commit();
        assertNeverWrongRows(ids(ROWS, true), false);
    }

    /**
     * A wrong recorded length must never produce wrong rows: each read either fails or returns exactly {@code correct}.
     * Which of the two happens depends on the storage client and the file size, so both are allowed; the outcome of
     * every read is printed, and {@code mustFailThroughTrustingIo} pins the case where only Parquet's footer check is
     * left to catch it.
     */
    private void assertNeverWrongRows(List<Integer> correct, boolean mustFailThroughTrustingIo) throws Exception {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        Assert.assertEquals("planning must succeed, so that only the read can fail", 1, tasks.size());
        boolean deleteCase = table.currentSnapshot().deleteManifests(table.io()).size() > 0;
        Assert.assertEquals("a delete file under test must be attached to the task, or nothing reads it",
                deleteCase ? 1 : 0, tasks.get(0).deletes().size());
        for (FileIO io : new FileIO[] { table.io(), new TrustingLengthFileIO(table.io()) }) {
            for (IcebergDeleteCache cache : new IcebergDeleteCache[] { null, new IcebergDeleteCache() }) {
                String how = io.getClass().getSimpleName() + (cache == null ? " without" : " with") + " the cache";
                List<Integer> rows;
                try {
                    rows = readAll(io, cache);
                } catch (Exception failed) {
                    Throwable root = failed;
                    while (root.getCause() != null) {
                        root = root.getCause();
                    }
                    System.out.println("WRONG-LENGTH OUTCOME " + how + ": failed, " + root.getClass().getSimpleName()
                            + ": " + root.getMessage());
                    continue;
                }
                System.out.println("WRONG-LENGTH OUTCOME " + how + ": returned " + rows.size() + " rows");
                Assert.assertFalse("only a failure can stop the trusting read here (" + how + ")",
                        mustFailThroughTrustingIo && io instanceof TrustingLengthFileIO);
                java.util.Collections.sort(rows);
                Assert.assertEquals("a wrong recorded length returned wrong rows (" + how + ")", correct, rows);
            }
        }
    }

    /** Ids 0..rows-1, without those in bucket 1 when the bucket delete applies. */
    private static List<Integer> ids(int rows, boolean bucketOneDeleted) {
        List<Integer> ids = new ArrayList<>();
        for (int id = 0; id < rows; id++) {
            if (!bucketOneDeleted || id % 5 != 1) {
                ids.add(id);
            }
        }
        return ids;
    }

    private List<Integer> readAll(FileIO io, IcebergDeleteCache cache) throws Exception {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        List<Integer> ids = new ArrayList<>();
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

    private DataFile writeDataFile(long lengthSkew) throws Exception {
        return writeDataFile(lengthSkew, ROWS, 0);
    }

    private static String pad(java.util.Random random, int bytes) {
        StringBuilder sb = new StringBuilder(bytes);
        for (int i = 0; i < bytes; i++) {
            sb.append((char) ('!' + random.nextInt(90)));
        }
        return sb.toString();
    }

    private DeleteFile writeLargeDelete(long lengthSkew) throws Exception {
        Schema deleteSchema = SCHEMA.select("pad");
        File file = new File(warehouse, "data/eq-delete-large.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(3)).overwrite().buildEqualityWriter();
        java.util.Random random = new java.util.Random(7);
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int i = 0; i < LARGE_ROWS; i++) {
                GenericRecord row = GenericRecord.create(deleteSchema);
                row.setField("pad", pad(random, PAD_BYTES));
                open.write(row);
            }
        }
        DeleteFile written = writer.toDeleteFile();
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned()).copy(written).ofEqualityDeletes(3)
                .withFileSizeInBytes(written.fileSizeInBytes() + lengthSkew).build();
    }

    private DataFile writeDataFile(long lengthSkew, int rows, int padBytes) throws Exception {
        File file = new File(warehouse, "data/part.parquet");
        java.util.Random random = new java.util.Random(11);
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).overwrite().build();
        try (FileAppender<Record> writer = appender) {
            for (int id = 0; id < rows; id++) {
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("id", id);
                record.setField("bucket", id % 5);
                record.setField("pad", padBytes == 0 ? null : pad(random, padBytes));
                writer.add(record);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned()).withPath(file.getAbsolutePath())
                .withFileSizeInBytes(file.length() + lengthSkew).withMetrics(appender.metrics())
                .withFormat(FileFormat.PARQUET).build();
    }

    private DeleteFile writeDelete(long lengthSkew) throws Exception {
        Schema deleteSchema = SCHEMA.select("bucket");
        File file = new File(warehouse, "data/eq-delete.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(2)).overwrite().buildEqualityWriter();
        try (EqualityDeleteWriter<Record> open = writer) {
            GenericRecord row = GenericRecord.create(deleteSchema);
            row.setField("bucket", 1);
            open.write(row);
        }
        DeleteFile written = writer.toDeleteFile();
        if (lengthSkew == 0) {
            return written;
        }
        // copy() keeps the content kind but not the equality field ids, which planning needs
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned()).copy(written).ofEqualityDeletes(2)
                .withFileSizeInBytes(written.fileSizeInBytes() + lengthSkew).build();
    }

    /**
     * Takes the length it is given on trust, as the object store clients do, instead of checking it against the file
     * the way Hadoop's local file system does; with this, only Parquet's own footer check can catch a wrong length.
     */
    private static final class TrustingLengthFileIO implements FileIO {
        private final FileIO delegate;

        TrustingLengthFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile newInputFile(String path) {
            return delegate.newInputFile(path);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            InputFile local = org.apache.iceberg.Files
                    .localInput(new File(java.net.URI.create(path.startsWith("file:") ? path : "file:" + path)));
            return new InputFile() {
                @Override
                public long getLength() {
                    return length;
                }

                @Override
                public org.apache.iceberg.io.SeekableInputStream newStream() {
                    return local.newStream();
                }

                @Override
                public String location() {
                    return path;
                }

                @Override
                public boolean exists() {
                    return local.exists();
                }
            };
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }
    }
}
