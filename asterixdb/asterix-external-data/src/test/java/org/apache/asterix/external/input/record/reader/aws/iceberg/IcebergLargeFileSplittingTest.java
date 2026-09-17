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
package org.apache.asterix.external.input.record.reader.aws.iceberg;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Splitting over a file of many real Parquet row groups: ten megabytes written as ten one-megabyte row groups, so a
 * data file is genuinely divided at row-group boundaries rather than at the synthetic eight-kilobyte ones
 * {@link IcebergScanTaskSplittingTest} uses.
 * <p>
 * The row-group size is set explicitly rather than left at Parquet's 128 MB default, because that default would put
 * this whole file in a single row group and nothing could split. The trade is deliberate: a gigabyte fixture would
 * exercise the production default but costs far more on every CI run, and the properties under test -- tiling,
 * a row group larger than the target, coalescing, and deletes across split boundaries -- do not depend on the
 * absolute sizes, only on there being several row groups.
 * <p>
 * Ordered: the deletes are committed after the plain reads, on the same file, so the fixture is written once.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Writes a 10 MB Parquet file as ten 1 MB row groups, then asserts it splits at the table's target and reads every row exactly once at three targets, before and after position deletes")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IcebergLargeFileSplittingTest {

    private static final long ONE_MB = 1024L * 1024L;
    private static final long TARGET_FILE_BYTES = 10L * ONE_MB;
    /** Explicit, so the file has several row groups; Parquet's 128 MB default would make it exactly one. */
    private static final long ROW_GROUP_BYTES = ONE_MB;
    /** The table's read.split.target-size: one row group, so the default target does not have to fit the fixture. */
    private static final long TABLE_SPLIT_SIZE = ONE_MB;
    private static final int PAYLOAD_CHARS = 1000;
    /** Payloads are slices of this many random letters, so writing costs no per-row randomness. */
    private static final int RANDOM_POOL_CHARS = 8 * 1024 * 1024;

    /** Positions in the first row group, deep inside the file, and (patched at run time) the very last row. */
    private static final long[] DELETED_POSITIONS = { 0, 1, 2_500, 6_000, 9_998 };

    private static File tempDir;
    private static Table table;
    private static DataFile dataFile;
    private static int rowCount;
    private static long lastPosition;

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
    }

    @BeforeClass
    public static void writeGigabyteFile() throws IOException {
        tempDir = Files.createTempDirectory("split-large").toFile();
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(TableProperties.SPLIT_SIZE, Long.toString(TABLE_SPLIT_SIZE));
        table = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema(),
                PartitionSpec.unpartitioned(), props, new File(tempDir, "tbl").getAbsolutePath());

        String path = table.location() + "/data/large.parquet";
        OutputFile out = table.io().newOutputFile(path);
        byte[] pool = new byte[RANDOM_POOL_CHARS];
        Random random = new Random(42);
        for (int i = 0; i < pool.length; i++) {
            pool[i] = (byte) ('a' + random.nextInt(26));
        }
        GenericRecord template = GenericRecord.create(table.schema());
        long started = System.nanoTime();
        int rows = 0;
        // Uncompressed so the bytes on disk are the bytes written, and the row groups are small enough that a
        // ten-megabyte file still has ten of them to split between.
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
                        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(ROW_GROUP_BYTES))
                        .createWriterFunc(GenericParquetWriter::create).build()) {
            while (writer.length() < TARGET_FILE_BYTES) {
                Record row = template.copy();
                row.setField("id", rows);
                int offset = random.nextInt(RANDOM_POOL_CHARS - PAYLOAD_CHARS);
                row.setField("payload", new String(pool, offset, PAYLOAD_CHARS, StandardCharsets.US_ASCII));
                writer.add(row);
                rows++;
            }
            writer.close();
            rowCount = rows;
            lastPosition = rows - 1;
            long bytes = out.toInputFile().getLength();
            dataFile = DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(bytes).withRecordCount(rows).withMetrics(writer.metrics())
                    .withSplitOffsets(writer.splitOffsets()).build();
            report("wrote %d rows, %d MB, %d row groups in %d ms", rows, bytes / ONE_MB, writer.splitOffsets().size(),
                    (System.nanoTime() - started) / 1_000_000);
        }
        table.newAppend().appendFile(dataFile).commit();

        Assert.assertTrue("the file must reach the target size", dataFile.fileSizeInBytes() >= TARGET_FILE_BYTES);
        Assert.assertTrue(
                "the configured row-group size must give several row groups, got " + dataFile.splitOffsets().size(),
                dataFile.splitOffsets().size() >= 4);
        for (long pos : DELETED_POSITIONS) {
            Assert.assertTrue("deleted position " + pos + " must exist", pos <= lastPosition);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        if (tempDir != null) {
            try (var paths = Files.walk(tempDir.toPath())) {
                paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
            }
        }
    }

    /** The target the table carries is what the factory gets, and it does split this file. */
    @Test
    public void test1_tableTargetSplitsTheFile() throws Exception {
        long target = table.newScan().targetSplitSize();
        Assert.assertEquals(TABLE_SPLIT_SIZE, target);

        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned(), target);
        report("table target %d KB: %d splits for %d row groups", target / 1024, splits.size(),
                dataFile.splitOffsets().size());
        Assert.assertTrue("the file must split at the table's target", splits.size() > 1);
        assertTiling(splits);
        assertEveryRowExactlyOnce(splits, Collections.emptySet(), "table target");
    }

    /**
     * A target smaller than a row group: every row group becomes its own split, each larger than the target, and
     * the read is still exact. This is what a table with big row groups and a small {@code read.split.target-size}
     * produces.
     */
    @Test
    public void test2_rowGroupLargerThanTargetStaysWhole() throws Exception {
        long target = ROW_GROUP_BYTES / 4;
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned(), target);
        Assert.assertEquals("one split per row group", dataFile.splitOffsets().size(), splits.size());
        for (FileScanTask split : splits) {
            Assert.assertTrue("a whole row group exceeds this target", split.length() > target);
        }
        assertTiling(splits);
        assertEveryRowExactlyOnce(splits, Collections.emptySet(), "quarter-row-group target");
    }

    /** A target of several row groups: adjacent row groups coalesce, so there are fewer splits than row groups. */
    @Test
    public void test3_rowGroupsCoalesceUpToTheTarget() throws Exception {
        long target = 3 * ROW_GROUP_BYTES;
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned(), target);
        Assert.assertTrue("still split", splits.size() > 1);
        Assert.assertTrue("fewer splits than row groups", splits.size() < dataFile.splitOffsets().size());
        for (FileScanTask split : splits) {
            Assert.assertTrue("a coalesced split stays within the target", split.length() <= target);
        }
        assertTiling(splits);
        assertEveryRowExactlyOnce(splits, Collections.emptySet(), "three-row-group target");
    }

    /** Position deletes across the file, applied through splits that start well inside it. */
    @Test
    public void test4_positionDeletesAcrossSplits() throws Exception {
        long[] positions = DELETED_POSITIONS.clone();
        positions[positions.length - 1] = lastPosition;
        OutputFile out = table.io().newOutputFile(table.location() + "/deletes/pos.parquet");
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(out).withSpec(table.spec()).buildPositionWriter();
        try (writer) {
            PositionDelete<Record> delete = PositionDelete.create();
            for (long pos : positions) {
                writer.write(delete.set(dataFile.location(), pos));
            }
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();

        Set<Integer> deleted = new HashSet<>();
        for (long pos : positions) {
            deleted.add((int) pos);
        }
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(), TABLE_SPLIT_SIZE);
        for (FileScanTask split : splits) {
            Assert.assertEquals(1, split.deletes().size());
        }
        assertEveryRowExactlyOnce(splits, deleted, "table target with position deletes");
    }

    /** Sizes and timings go to standard output, which the test runner shows; the module has no test log config. */
    private static void report(String format, Object... args) {
        System.out.println(
                "[" + IcebergLargeFileSplittingTest.class.getSimpleName() + "] " + String.format(format, args));
    }

    private static List<FileScanTask> planned() throws IOException {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        Assert.assertEquals(1, tasks.size());
        return tasks;
    }

    private static void assertTiling(List<FileScanTask> splits) {
        List<FileScanTask> sorted = new ArrayList<>(splits);
        sorted.sort(Comparator.comparingLong(FileScanTask::start));
        long expectedStart = dataFile.splitOffsets().get(0);
        Set<Long> boundaries = new HashSet<>(dataFile.splitOffsets());
        for (FileScanTask split : sorted) {
            Assert.assertEquals("contiguous", expectedStart, split.start());
            Assert.assertTrue("on a row-group boundary", boundaries.contains(split.start()));
            expectedStart += split.length();
        }
        Assert.assertEquals("ends at the file size", dataFile.fileSizeInBytes(), expectedStart);
    }

    /** Reads only {@code id} through the reader's standard path over every split; reports how long the pass took. */
    private static void assertEveryRowExactlyOnce(List<FileScanTask> splits, Set<Integer> deleted, String label)
            throws IOException {
        Schema idOnly = table.schema().select("id");
        long started = System.nanoTime();
        List<Integer> ids = new ArrayList<>(rowCount);
        int nonEmptySplits = 0;
        for (FileScanTask split : splits) {
            List<Integer> splitIds = IcebergScanTaskSplittingTest.readIds(table, split, idOnly);
            Assert.assertTrue("no split reads the whole file", splitIds.size() < rowCount);
            if (!splitIds.isEmpty()) {
                nonEmptySplits++;
            }
            ids.addAll(splitIds);
        }
        report("%s: read %d rows over %d splits (%d non-empty) in %d ms", label, ids.size(), splits.size(),
                nonEmptySplits, (System.nanoTime() - started) / 1_000_000);
        Assert.assertTrue("rows are spread over several splits", nonEmptySplits > 1);

        Collections.sort(ids);
        List<Integer> expected = new ArrayList<>(rowCount);
        for (int id = 0; id < rowCount; id++) {
            if (!deleted.contains(id)) {
                expected.add(id);
            }
        }
        Assert.assertEquals(expected, ids);
    }
}
