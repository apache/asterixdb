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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.asterix.external.input.record.reader.aws.iceberg.IcebergParquetRecordReaderFactory.PartitionWorkLoadBasedOnSize;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Splitting large data files across scan tasks, checked end to end over tables generated on the fly.
 * <p>
 * The tables are written with a tiny Parquet row-group size and read with a tiny target split size, so a file of a
 * few hundred kilobytes stands in for a multi-gigabyte one: it holds many row groups and splits into several tasks.
 * Every read below goes through the exact calls the record reader makes, and is compared against the unsplit read of
 * the same file, so the property under test is always "the splits together return exactly the rows the whole file
 * does", never a count that could pass by coincidence. {@link IcebergLargeFileSplittingTest} repeats the core of this
 * at real scale, with Iceberg's default sizes and a file over a gigabyte.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Generates multi-row-group Parquet files into temp Iceberg tables and asserts split tasks tile each file exactly, read every row exactly once on the plain, deletes (position files, equality, deletion vector) and variant-pruned paths with and without a residual, pack by sizeBytes and survive Java serialization")
public class IcebergScanTaskSplittingTest {

    private static final long TARGET_SPLIT_SIZE = 64 * 1024;
    private static final int ROW_GROUP_BYTES = 8 * 1024;
    private static final int PAYLOAD_CHARS = 40;

    private static final int BIG_ROWS = 20_000;
    private static final int SMALL_FIRST_ID = 50_000;
    private static final int SMALL_ROWS = 100;
    private static final int NO_OFFSETS_FIRST_ID = 100_000;
    private static final int NO_OFFSETS_ROWS = 10_000;
    private static final int RESIDUAL_MIN_ID = 15_000;

    /** Row positions, spread over the first, middle and last row groups so every split sees at least one. */
    private static final long[] DELETED_POSITIONS = { 0, 1, 4_999, 5_000, 9_999, 12_345, 19_998, 19_999 };
    private static final int[] EQUALITY_DELETED_IDS = { 7, 15_000 };

    private static final String VARIANT_COLUMN = "variant_field";
    private static final int BUCKETS = 97;

    private static final List<File> TEMP_DIRS = new ArrayList<>();

    private static Table plainTable;
    private static Table emptyTable;
    private static Table deletesTable;
    private static Table deletionVectorTable;
    private static Table variantTable;

    private static Schema plainSchema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
    }

    private static Schema variantSchema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, VARIANT_COLUMN, Types.VariantType.get()));
    }

    @BeforeClass
    public static void setup() throws Exception {
        // Version 2: position deletes are plain files there, which is what the reader's delete path consumes.
        plainTable = createTable("plain", plainSchema(), 2);
        DataFile big = writeDataFile(plainTable, "big", 0, BIG_ROWS, true);
        DataFile small = writeDataFile(plainTable, "small", SMALL_FIRST_ID, SMALL_ROWS, true);
        DataFile noOffsets = writeDataFile(plainTable, "no_offsets", NO_OFFSETS_FIRST_ID, NO_OFFSETS_ROWS, false);
        plainTable.newAppend().appendFile(big).appendFile(small).appendFile(noOffsets).commit();
        Assert.assertTrue("the big file must hold many row groups for splitting to be exercised",
                big.splitOffsets().size() > 1);

        emptyTable = createTable("empty", plainSchema(), 2);
        emptyTable.newAppend().appendFile(writeEmptyDataFile(emptyTable)).commit();

        // Position deletes on a file with recorded offsets and on one without, so both boundary strategies are
        // checked against absolute row positions. The equality delete applies to every file.
        deletesTable = createTable("deletes", plainSchema(), 2);
        DataFile deletesBig = writeDataFile(deletesTable, "big", 0, BIG_ROWS, true);
        DataFile deletesNoOffsets = writeDataFile(deletesTable, "big_no_offsets", NO_OFFSETS_FIRST_ID, BIG_ROWS, false);
        deletesTable.newAppend().appendFile(deletesBig).appendFile(deletesNoOffsets).commit();
        RowDelta delta = deletesTable.newRowDelta();
        delta.addDeletes(writePositionDeletes(deletesTable, deletesBig, "pos_big", DELETED_POSITIONS));
        delta.addDeletes(writePositionDeletes(deletesTable, deletesNoOffsets, "pos_no_offsets", DELETED_POSITIONS));
        delta.addDeletes(writeEqualityDeletes(deletesTable, EQUALITY_DELETED_IDS));
        delta.commit();

        // Version 3: the same positions deleted through a deletion vector, the delete shape v3 tables produce.
        deletionVectorTable = createTable("dv", plainSchema(), 3);
        DataFile dvBig = writeDataFile(deletionVectorTable, "big", 0, BIG_ROWS, true);
        deletionVectorTable.newAppend().appendFile(dvBig).commit();
        RowDelta dvDelta = deletionVectorTable.newRowDelta();
        for (DeleteFile dv : writeDeletionVector(deletionVectorTable, dvBig, DELETED_POSITIONS)) {
            dvDelta.addDeletes(dv);
        }
        dvDelta.commit();

        // Variant needs format version 3.
        variantTable = createTable("variant", variantSchema(), 3);
        variantTable.newAppend().appendFile(writeShreddedVariantFile(variantTable, BIG_ROWS)).commit();

    }

    @AfterClass
    public static void cleanup() throws IOException {
        for (File dir : TEMP_DIRS) {
            try (var paths = Files.walk(dir.toPath())) {
                paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
            }
        }
    }

    /** The table property is what the factory hands to {@code splitTasks}, via {@code scan.targetSplitSize()}. */
    @Test
    public void targetSplitSizeComesFromTheTable() {
        Assert.assertEquals(TARGET_SPLIT_SIZE, plainTable.newScan().targetSplitSize());
    }

    /**
     * The splits of a file tile it: contiguous, starting at the first row group, ending at the file size, with no
     * gaps or overlaps. A file under the target size stays one task, and a file with recorded split offsets breaks
     * only at them, with adjacent row groups coalesced up to the target.
     */
    @Test
    public void splitsTileEachFileExactly() throws Exception {
        List<FileScanTask> planned = planned(plainTable);
        Assert.assertEquals(3, planned.size());
        Map<String, FileScanTask> byPath = new HashMap<>();
        planned.forEach(t -> byPath.put(t.file().location(), t));

        Map<String, List<FileScanTask>> splitsByPath =
                groupByFile(IcebergParquetRecordReaderFactory.splitTasks(planned, TARGET_SPLIT_SIZE));
        Assert.assertEquals("every planned file is still present", byPath.keySet(), splitsByPath.keySet());

        for (Map.Entry<String, List<FileScanTask>> e : splitsByPath.entrySet()) {
            FileScanTask whole = byPath.get(e.getKey());
            List<FileScanTask> splits = e.getValue();
            splits.sort(Comparator.comparingLong(FileScanTask::start));
            // With recorded offsets the first split starts at the first row group, past the 4-byte Parquet magic;
            // no row-group midpoint can fall in those bytes, so they belong to no split and nothing is lost.
            long expectedStart = firstSplitStart(whole);
            for (FileScanTask split : splits) {
                Assert.assertEquals("splits of " + e.getKey() + " must be contiguous", expectedStart, split.start());
                Assert.assertTrue(split.length() > 0);
                Assert.assertEquals("a split keeps its file", whole.file().location(), split.file().location());
                expectedStart += split.length();
            }
            Assert.assertEquals("splits of " + e.getKey() + " must end at the file size", whole.length(),
                    expectedStart);

            String name = new File(e.getKey()).getName();
            if (name.startsWith("small")) {
                Assert.assertEquals("a file under the target size is not split", 1, splits.size());
            } else {
                Assert.assertTrue(name + " must be split, got " + splits.size(), splits.size() > 1);
            }
            if (name.startsWith("big")) {
                List<Long> offsets = whole.file().splitOffsets();
                Set<Long> boundaries = new HashSet<>(offsets);
                Assert.assertTrue("row groups are coalesced up to the target, not one split per row group",
                        splits.size() < offsets.size());
                for (FileScanTask split : splits) {
                    Assert.assertTrue("a split with recorded offsets starts on a row-group boundary: " + split.start(),
                            boundaries.contains(split.start()));
                    long end = split.start() + split.length();
                    long rowGroups = offsets.stream().filter(o -> o >= split.start() && o < end).count();
                    Assert.assertTrue("a split stays within the target unless it is a single row group",
                            split.length() <= TARGET_SPLIT_SIZE || rowGroups == 1);
                    Assert.assertTrue("a split holds whole row groups", rowGroups >= 1);
                }
            }
        }
    }

    /**
     * At Iceberg's real default target, which no fixture file approaches, every file stays one task covering
     * all of it: this is the no-change guarantee for small-file tables.
     */
    @Test
    public void nothingIsSplitBelowTheTarget() throws Exception {
        List<FileScanTask> planned = planned(plainTable);
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned, TableProperties.SPLIT_SIZE_DEFAULT);
        Assert.assertEquals(planned.size(), splits.size());
        for (FileScanTask split : splits) {
            Assert.assertEquals(firstSplitStart(split), split.start());
            Assert.assertEquals(split.file().fileSizeInBytes(), split.start() + split.length());
        }
    }

    /**
     * A non-positive {@code read.split.target-size} must be rejected outright.
     * <p>
     * The property reaches the scan through {@code Long.parseLong} with no range check, and a file that records no
     * split offsets takes Iceberg's fixed-size iterator, which advances by {@code min(size, remaining)}. Measured on
     * such a file: a target of {@code 0} or {@code -1} builds empty tasks until the heap is exhausted, and
     * {@code Long.MIN_VALUE} yields one task of negative length that reads none of the file's rows and reports no
     * error. The timeout is part of the assertion: a regression here hangs rather than fails.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins the non-positive split-size guard, whose absence was measured to exhaust the heap on a data file without recorded split offsets")
    @Test(timeout = 60000)
    public void nonPositiveTargetSplitSizeIsRejected() throws Exception {
        List<FileScanTask> planned = planned(plainTable);
        for (long bad : new long[] { 0, -1, Long.MIN_VALUE }) {
            try {
                IcebergParquetRecordReaderFactory.splitTasks(planned, bad);
                Assert.fail("split size " + bad + " must be rejected");
            } catch (IllegalArgumentException expected) {
                Assert.assertTrue("message should name the offending size, got: " + expected.getMessage(),
                        expected.getMessage().contains("Split size must be > 0"));
            }
        }
    }

    /**
     * The WITH-clause switch defaults to on, and only the literal "true" keeps it on. Anything else -- including a
     * misspelling -- reads as off, which is how the two variant pushdown flags behave; the point of the test is that
     * the three flags stay consistent with each other.
     */
    @Test(timeout = 60000)
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Covers the splitScanTasks flag default and parsing")
    public void splitScanTasksFlagDefaultsToOn() {
        Assert.assertTrue("absent means on",
                IcebergParquetRecordReaderFactory.isSplitScanTasksEnabled(new HashMap<>()));
        Assert.assertTrue("explicit true is on",
                IcebergParquetRecordReaderFactory.isSplitScanTasksEnabled(Map.of("splitScanTasks", "true")));
        Assert.assertTrue("case is not significant",
                IcebergParquetRecordReaderFactory.isSplitScanTasksEnabled(Map.of("splitScanTasks", "TRUE")));
        Assert.assertFalse("explicit false is off",
                IcebergParquetRecordReaderFactory.isSplitScanTasksEnabled(Map.of("splitScanTasks", "false")));
        Assert.assertTrue("the default constant must stay on",
                ExternalDataConstants.IcebergOptions.DEFAULT_SPLIT_SCAN_TASKS);
    }

    /**
     * The flag must reach the planning decision, not merely parse. Same planned input, same target size, only the
     * configuration differs: off yields one task per data file, on yields more, and both read the same rows.
     * <p>
     * Without this the gate and the splitting are only tested apart, so removing the gate from planning would leave
     * every other test in this class green.
     */
    @Test(timeout = 60000)
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Asserts the splitScanTasks flag controls planning, not just that it parses")
    public void splitScanTasksFlagControlsPlanning() throws Exception {
        List<FileScanTask> planned = planned(plainTable);
        Assert.assertFalse("fixture must plan at least one file", planned.isEmpty());

        List<FileScanTask> off = IcebergParquetRecordReaderFactory.planScanTasks(planned, TARGET_SPLIT_SIZE,
                Map.of("splitScanTasks", "false"));
        List<FileScanTask> on = IcebergParquetRecordReaderFactory.planScanTasks(planned, TARGET_SPLIT_SIZE,
                Map.of("splitScanTasks", "true"));
        List<FileScanTask> byDefault =
                IcebergParquetRecordReaderFactory.planScanTasks(planned, TARGET_SPLIT_SIZE, new HashMap<>());

        Assert.assertEquals("off must plan one task per data file", planned.size(), off.size());
        Assert.assertTrue("on must plan more tasks than files, got " + on.size() + " from " + planned.size(),
                on.size() > planned.size());
        Assert.assertEquals("absent must behave as on", on.size(), byDefault.size());

        List<Integer> offIds = new ArrayList<>();
        for (FileScanTask task : off) {
            offIds.addAll(readIds(plainTable, task, plainTable.schema()));
        }
        List<Integer> onIds = new ArrayList<>();
        for (FileScanTask task : on) {
            onIds.addAll(readIds(plainTable, task, plainTable.schema()));
        }
        Collections.sort(offIds);
        Collections.sort(onIds);
        Assert.assertEquals("the flag may change the plan, never the rows", offIds, onIds);
    }

    /**
     * With the switch off, planning must fall back to exactly Iceberg's own tasks -- one per data file -- and reading
     * them must still return every row. This is the differential oracle the flag exists to provide.
     */
    @Test(timeout = 60000)
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Splitting off yields one task per data file and the same rows as splitting on")
    public void splittingOffYieldsOneTaskPerFileAndSameRows() throws Exception {
        List<FileScanTask> planned = planned(plainTable);
        Assert.assertFalse("fixture must plan at least one file", planned.isEmpty());

        List<FileScanTask> split = IcebergParquetRecordReaderFactory.splitTasks(planned, TARGET_SPLIT_SIZE);
        Assert.assertTrue("fixture must actually split when the flag is on, got " + split.size() + " task(s)",
                split.size() > planned.size());

        List<Integer> unsplitIds = new ArrayList<>();
        for (FileScanTask task : planned) {
            unsplitIds.addAll(readIds(plainTable, task, plainTable.schema()));
        }
        List<Integer> splitIds = new ArrayList<>();
        for (FileScanTask task : split) {
            splitIds.addAll(readIds(plainTable, task, plainTable.schema()));
        }
        Collections.sort(unsplitIds);
        Collections.sort(splitIds);
        Assert.assertEquals("splitting must not change which rows are read", unsplitIds, splitIds);
    }

    /** A data file with no rows has no row groups; it must neither fail nor produce rows, however it is split. */
    @Test
    public void emptyFileReadsNothing() throws Exception {
        List<FileScanTask> planned = planned(emptyTable);
        Assert.assertEquals(1, planned.size());
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned, TARGET_SPLIT_SIZE);
        Assert.assertTrue("an empty file yields at most one task, got " + splits.size(), splits.size() <= 1);
        for (FileScanTask split : splits) {
            Assert.assertEquals(Collections.emptyList(), readIds(emptyTable, split, emptyTable.schema()));
        }
    }

    /**
     * Reading every split through the reader's standard path returns each row exactly once: no row lost at a split
     * boundary, none read by two splits. Covers both boundary strategies, since one file has recorded split offsets
     * and one does not, and checks that the splits genuinely partition the rows rather than one split reading all.
     */
    @Test
    public void splitReadsReturnEveryRowExactlyOnce() throws Exception {
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(plainTable), TARGET_SPLIT_SIZE);
        List<Integer> ids = new ArrayList<>();
        int nonEmptyBigSplits = 0;
        for (FileScanTask split : splits) {
            List<Integer> splitIds = readIds(plainTable, split, plainTable.schema());
            if (new File(split.file().location()).getName().startsWith("big")) {
                Assert.assertTrue("no split of the big file reads the whole file", splitIds.size() < BIG_ROWS);
                if (!splitIds.isEmpty()) {
                    nonEmptyBigSplits++;
                }
            }
            ids.addAll(splitIds);
        }
        Assert.assertTrue("the big file's rows are spread over several splits", nonEmptyBigSplits > 1);

        List<Integer> expected = new ArrayList<>();
        addRange(expected, 0, BIG_ROWS, Collections.emptySet());
        addRange(expected, SMALL_FIRST_ID, SMALL_ROWS, Collections.emptySet());
        addRange(expected, NO_OFFSETS_FIRST_ID, NO_OFFSETS_ROWS, Collections.emptySet());
        Collections.sort(ids);
        Assert.assertEquals(expected, ids);
    }

    /**
     * Position deletes name a row by its absolute position in the file. A split that starts mid-file must still
     * drop exactly those rows, whether its boundaries follow recorded row-group offsets or are fixed byte ranges that
     * fall mid-row-group, and an equality delete must apply in every split. The split tasks carry the delete files,
     * and their {@code sizeBytes()} is larger than their data length because of them.
     */
    @Test
    public void splitReadsHonourPositionAndEqualityDeletes() throws Exception {
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(deletesTable), TARGET_SPLIT_SIZE);
        Assert.assertTrue("the deletes files must be split for this to test anything",
                groupByFile(splits).values().stream().allMatch(s -> s.size() > 1));

        List<Integer> ids = new ArrayList<>();
        for (FileScanTask split : splits) {
            // What Iceberg attaches is decided by bounds. The position-delete files here carry no file-path bounds, so
            // both are attached to both data files, and the one for the other file must then delete nothing. The
            // equality delete is attached only where the deleted ids overlap the file's id bounds, so only to the
            // first file. The row assertion below checks that both decisions are honoured by the split reads.
            boolean firstFile = new File(split.file().location()).getName().equals("big.parquet");
            long equality = split.deletes().stream().filter(d -> d.content() == FileContent.EQUALITY_DELETES).count();
            long position = split.deletes().stream().filter(d -> d.content() == FileContent.POSITION_DELETES).count();
            String attached = split.deletes().stream().map(d -> d.content() + ":" + new File(d.location()).getName())
                    .sorted().collect(Collectors.joining(", "));
            Assert.assertEquals("equality delete attached only where the ids overlap, got " + attached,
                    firstFile ? 1 : 0, equality);
            Assert.assertEquals("every split carries both position-delete files, got " + attached, 2, position);
            Assert.assertTrue("sizeBytes accounts for the delete files", split.sizeBytes() > split.length());
            ids.addAll(readIds(deletesTable, split, deletesTable.schema()));
        }

        Collections.sort(ids);
        Assert.assertEquals(expectedAfterDeletes(), ids);
    }

    /** The version-3 shape of the same deletes: one deletion vector on the file, applied in every split. */
    @Test
    public void splitReadsHonourDeletionVectors() throws Exception {
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(deletionVectorTable), TARGET_SPLIT_SIZE);
        Assert.assertTrue("the file must be split for this to test anything", splits.size() > 1);

        List<Integer> ids = new ArrayList<>();
        for (FileScanTask split : splits) {
            Assert.assertEquals("every split carries the deletion vector", 1, split.deletes().size());
            Assert.assertEquals(FileFormat.PUFFIN, split.deletes().get(0).format());
            Assert.assertTrue("sizeBytes accounts for the deletion vector", split.sizeBytes() > split.length());
            ids.addAll(readIds(deletionVectorTable, split, deletionVectorTable.schema()));
        }

        List<Integer> expected = new ArrayList<>();
        addRange(expected, 0, BIG_ROWS, positionsAsIds(0));
        Collections.sort(ids);
        Assert.assertEquals(expected, ids);
    }

    /**
     * A pushed-down filter reaches each split as its residual, which Iceberg applies to row-group skipping. The
     * splits together must still return every matching row exactly once, and must skip something, or the residual
     * was not applied. Rows the engine would filter out may be returned; duplicates and losses may not.
     */
    @Test
    public void residualIsAppliedPerSplitOnTheStandardPath() throws Exception {
        TableScan scan = plainTable.newScan().filter(Expressions.greaterThanOrEqual("id", RESIDUAL_MIN_ID));
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned(scan), TARGET_SPLIT_SIZE);
        Assert.assertFalse(splits.isEmpty());

        List<Integer> ids = new ArrayList<>();
        for (FileScanTask split : splits) {
            Assert.assertNotEquals("the filter must survive as a residual", Expressions.alwaysTrue(), split.residual());
            ids.addAll(readIds(plainTable, split, plainTable.schema()));
        }
        assertResidualRead(ids, BIG_ROWS + SMALL_ROWS + NO_OFFSETS_ROWS);
    }

    /** The same residual check for our own row-group loop in the variant-pruned reader. */
    @Test
    public void residualIsAppliedPerSplitOnTheVariantPrunedPath() throws Exception {
        TableScan scan = variantTable.newScan().filter(Expressions.greaterThanOrEqual("id", RESIDUAL_MIN_ID));
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned(scan), TARGET_SPLIT_SIZE);
        Assert.assertTrue(splits.size() > 1);

        VariantProjectionPlan plan = bucketOnlyPlan();
        List<Integer> ids = new ArrayList<>();
        for (FileScanTask split : splits) {
            try (VariantProjectedParquetReader reader = VariantProjectedParquetReader.open(input(variantTable, split),
                    variantTable.schema(), split.residual(), split.start(), split.length(), true, plan)) {
                Assert.assertTrue(reader.canPrune());
                ids.addAll(readBuckets(reader).keySet());
            }
        }
        assertResidualRead(ids, BIG_ROWS);
    }

    /**
     * The variant-pruned reader is our own {@code ParquetFileReader} loop, so its range handling is not Iceberg's and
     * needs its own proof: over every split it must return exactly the rows and sub-field values the standard
     * unsplit read does, while actually pruning.
     */
    @Test
    public void variantPrunedReadOverSplitsMatchesTheWholeFile() throws Exception {
        List<FileScanTask> planned = planned(variantTable);
        Assert.assertEquals(1, planned.size());
        FileScanTask whole = planned.get(0);
        Map<Integer, Integer> expected = readBuckets(IcebergFileRecordReader.openStandardRead(variantTable.io(),
                input(variantTable, whole), whole, variantTable.schema(), variantTable.schema()));
        Assert.assertEquals(BIG_ROWS, expected.size());

        VariantProjectionPlan plan = bucketOnlyPlan();
        List<FileScanTask> splits = IcebergParquetRecordReaderFactory.splitTasks(planned, TARGET_SPLIT_SIZE);
        Assert.assertTrue("the variant file must be split for this to test anything", splits.size() > 1);

        Map<Integer, Integer> actual = new TreeMap<>();
        for (FileScanTask split : splits) {
            try (VariantProjectedParquetReader reader = VariantProjectedParquetReader.open(input(variantTable, split),
                    variantTable.schema(), split.residual(), split.start(), split.length(), true, plan)) {
                Assert.assertTrue("the fixture must actually prune", reader.canPrune());
                Map<Integer, Integer> splitBuckets = readBuckets(reader);
                Assert.assertTrue("no split reads the whole file", splitBuckets.size() < BIG_ROWS);
                for (Map.Entry<Integer, Integer> e : splitBuckets.entrySet()) {
                    Assert.assertNull("row " + e.getKey() + " read by two splits",
                            actual.put(e.getKey(), e.getValue()));
                }
            }
        }
        Assert.assertEquals(new TreeMap<>(expected), actual);
    }

    /**
     * Packing weighs by {@code sizeBytes()}, which includes the attached delete files, so a split with deletes is
     * not treated as if only its data bytes cost anything. Every task lands on exactly one partition and the greedy
     * packing keeps partitions within one task weight of each other. With more partitions than tasks the surplus
     * partitions simply get empty workloads.
     */
    @Test
    public void workloadsAreWeightedBySizeBytes() throws Exception {
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(deletesTable), TARGET_SPLIT_SIZE);
        int partitions = 3;
        List<PartitionWorkLoadBasedOnSize> workloads =
                IcebergParquetRecordReaderFactory.distributeWorkLoad(splits, partitions);
        Assert.assertEquals(partitions, workloads.size());

        List<FileScanTask> assigned = new ArrayList<>();
        long maxWeight = 0;
        for (PartitionWorkLoadBasedOnSize workload : workloads) {
            long bySizeBytes = 0;
            long byLength = 0;
            for (FileScanTask task : workload.getFileScanTasks()) {
                bySizeBytes += task.sizeBytes();
                byLength += task.length();
                maxWeight = Math.max(maxWeight, task.sizeBytes());
            }
            Assert.assertEquals("total is the sum of sizeBytes", bySizeBytes, workload.getTotalSize());
            if (!workload.getFileScanTasks().isEmpty()) {
                Assert.assertNotEquals("length would be the wrong weight here", byLength, workload.getTotalSize());
            }
            assigned.addAll(workload.getFileScanTasks());
        }
        Assert.assertEquals("every split is assigned exactly once", identities(splits), identities(assigned));

        long min = workloads.stream().mapToLong(PartitionWorkLoadBasedOnSize::getTotalSize).min().orElseThrow();
        long max = workloads.stream().mapToLong(PartitionWorkLoadBasedOnSize::getTotalSize).max().orElseThrow();
        Assert.assertTrue("least-loaded packing stays within one task weight", max - min <= maxWeight);

        int manyPartitions = splits.size() * 3;
        List<PartitionWorkLoadBasedOnSize> sparse =
                IcebergParquetRecordReaderFactory.distributeWorkLoad(splits, manyPartitions);
        Assert.assertEquals(manyPartitions, sparse.size());
        Assert.assertEquals(identities(splits),
                identities(sparse.stream().flatMap(w -> w.getFileScanTasks().stream()).collect(Collectors.toList())));
        Assert.assertEquals("surplus partitions are empty", manyPartitions - splits.size(),
                sparse.stream().filter(w -> w.getFileScanTasks().isEmpty()).count());
    }

    /**
     * Split tasks reach the worker nodes by Java serialization of the workloads. Iceberg's split task type declares
     * {@code Serializable} only through the {@code ScanTask} interface, so this pins that the round trip works and
     * keeps the range and deletes.
     */
    @Test
    public void workloadsWithSplitTasksSurviveSerialization() throws Exception {
        List<FileScanTask> splits =
                IcebergParquetRecordReaderFactory.splitTasks(planned(deletesTable), TARGET_SPLIT_SIZE);
        List<PartitionWorkLoadBasedOnSize> workloads = IcebergParquetRecordReaderFactory.distributeWorkLoad(splits, 2);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new ArrayList<>(workloads));
        }
        @SuppressWarnings("unchecked")
        List<PartitionWorkLoadBasedOnSize> restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (List<PartitionWorkLoadBasedOnSize>) in.readObject();
        }

        Assert.assertEquals(workloads.size(), restored.size());
        for (int i = 0; i < workloads.size(); i++) {
            Assert.assertEquals(workloads.get(i).getTotalSize(), restored.get(i).getTotalSize());
            Assert.assertEquals(identities(workloads.get(i).getFileScanTasks()),
                    identities(restored.get(i).getFileScanTasks()));
        }
        // And the restored tasks still read: the range and the deletes are intact, not just their description.
        List<Integer> ids = new ArrayList<>();
        for (PartitionWorkLoadBasedOnSize workload : restored) {
            for (FileScanTask task : workload.getFileScanTasks()) {
                ids.addAll(readIds(deletesTable, task, deletesTable.schema()));
            }
        }
        Collections.sort(ids);
        Assert.assertEquals(expectedAfterDeletes(), ids);
    }

    // ---- fixtures ----

    private static Table createTable(String name, Schema schema, int formatVersion) throws IOException {
        File dir = Files.createTempDirectory("split-" + name).toFile();
        TEMP_DIRS.add(dir);
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
        props.put(TableProperties.SPLIT_SIZE, Long.toString(TARGET_SPLIT_SIZE));
        return new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema,
                PartitionSpec.unpartitioned(), props, new File(dir, "tbl").getAbsolutePath());
    }

    /**
     * Writes {@code rows} rows with ids {@code firstId..} and a random payload, so the file does not collapse to a
     * handful of dictionary pages, in row groups of about {@link #ROW_GROUP_BYTES}. Registers the row-group offsets
     * on the data file only when asked, so the fixed-size fallback is exercised too.
     */
    private static DataFile writeDataFile(Table table, String name, int firstId, int rows, boolean recordSplitOffsets)
            throws IOException {
        String path = table.location() + "/data/" + name + ".parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        Random random = new Random(firstId);
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(ROW_GROUP_BYTES))
                .createWriterFunc(GenericParquetWriter::create).build()) {
            for (int i = 0; i < rows; i++) {
                Record row = template.copy();
                row.setField("id", firstId + i);
                row.setField("payload", randomPayload(random, PAYLOAD_CHARS));
                writer.add(row);
            }
            writer.close();
            DataFiles.Builder builder = DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(out.toInputFile().getLength()).withRecordCount(rows)
                    .withMetrics(writer.metrics());
            if (recordSplitOffsets) {
                builder.withSplitOffsets(writer.splitOffsets());
            }
            return builder.build();
        }
    }

    /**
     * A Parquet file with a footer and no row groups. Iceberg's own writer never produces one, it skips creating the
     * file when nothing was added, so this goes through parquet-mr directly, the way an externally written file added
     * to a table would have been produced. Field ids match the table schema so the reader binds columns by id.
     */
    private static DataFile writeEmptyDataFile(Table table) throws IOException {
        String path = table.location() + "/data/empty.parquet";
        MessageType type =
                org.apache.parquet.schema.Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).id(1)
                        .named("id").optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).id(2).named("payload").named("table");
        HadoopOutputFile out = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(path),
                new org.apache.hadoop.conf.Configuration());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(out).withType(type).build()) {
            // nothing written on purpose
        }
        return DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(table.io().newInputFile(path).getLength()).withRecordCount(0).build();
    }

    private static DeleteFile writePositionDeletes(Table table, DataFile dataFile, String name, long[] positions)
            throws IOException {
        OutputFile out = table.io().newOutputFile(table.location() + "/deletes/" + name + ".parquet");
        // Not forTable(..): that installs the table schema as a row schema, and these deletes carry no row data.
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(out).withSpec(table.spec()).buildPositionWriter();
        try (writer) {
            PositionDelete<Record> delete = PositionDelete.create();
            for (long pos : positions) {
                writer.write(delete.set(dataFile.location(), pos));
            }
        }
        return writer.toDeleteFile();
    }

    private static DeleteFile writeEqualityDeletes(Table table, int[] ids) throws IOException {
        Schema idOnly = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(table.location() + "/deletes/eq.parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(out).forTable(table).rowSchema(idOnly)
                .withSpec(table.spec()).equalityFieldIds(1).createWriterFunc(GenericParquetWriter::create)
                .buildEqualityWriter();
        try (writer) {
            GenericRecord template = GenericRecord.create(idOnly);
            for (int id : ids) {
                Record row = template.copy();
                row.setField("id", id);
                writer.write(row);
            }
        }
        return writer.toDeleteFile();
    }

    /** A Puffin deletion vector for {@code dataFile}, the way a v3 writer would produce it. */
    private static List<DeleteFile> writeDeletionVector(Table table, DataFile dataFile, long[] positions)
            throws IOException {
        OutputFileFactory files = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
        BaseDVFileWriter writer = new BaseDVFileWriter(files, path -> null);
        try (writer) {
            for (long pos : positions) {
                writer.delete(dataFile.location(), pos, table.spec(), null);
            }
        }
        return writer.result().deleteFiles();
    }

    /** One file of shredded {@code {bucket: int, name: string}} objects, many row groups, offsets recorded. */
    private static DataFile writeShreddedVariantFile(Table table, int rows) throws Exception {
        VariantMetadata meta = Variants.metadata("bucket", "name");
        // Shred to the layout of a representative value, the way the integration fixtures do.
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.parquet.schema.Type typed =
                (org.apache.parquet.schema.Type) toParquetSchema.invoke(null, variantOf(meta, 0, "sample").value());

        String path = table.location() + "/data/variant.parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        Random random = new Random(7);
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(ROW_GROUP_BYTES))
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fid, n) -> typed).build()) {
            for (int i = 0; i < rows; i++) {
                Record row = template.copy();
                row.setField("id", i);
                row.setField(VARIANT_COLUMN, variantOf(meta, i % BUCKETS, randomPayload(random, PAYLOAD_CHARS)));
                writer.add(row);
            }
            writer.close();
            Assert.assertTrue("the variant file must hold many row groups", writer.splitOffsets().size() > 1);
            return DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(out.toInputFile().getLength()).withRecordCount(rows)
                    .withMetrics(writer.metrics()).withSplitOffsets(writer.splitOffsets()).build();
        }
    }

    private static Variant variantOf(VariantMetadata meta, int bucket, String name) {
        ShreddedObject obj = Variants.object(meta);
        obj.put("bucket", Variants.of(bucket));
        obj.put("name", Variants.of(name));
        return Variant.of(meta, obj);
    }

    static String randomPayload(Random random, int chars) {
        byte[] bytes = new byte[chars];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ('a' + random.nextInt(26));
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    /** Requests only {@code variant_field.bucket} and {@code id}, so {@code name} is what pruning removes. */
    private static VariantProjectionPlan bucketOnlyPlan() throws Exception {
        ARecordType projected =
                ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of(VARIANT_COLUMN, "bucket"), List.of("id")));
        VariantProjectionPlan plan = VariantProjectionPlan.from(variantSchema(), projected, true);
        Assert.assertFalse("fixture must produce a narrowing plan", plan.isEmpty());
        return plan;
    }

    // ---- expectations ----

    /** The deletes table's surviving ids: both files minus the deleted positions, minus the equality-deleted ids. */
    private static List<Integer> expectedAfterDeletes() {
        Set<Integer> deleted = positionsAsIds(0);
        deleted.addAll(positionsAsIds(NO_OFFSETS_FIRST_ID));
        for (int id : EQUALITY_DELETED_IDS) {
            deleted.add(id);
        }
        List<Integer> expected = new ArrayList<>();
        addRange(expected, 0, BIG_ROWS, deleted);
        addRange(expected, NO_OFFSETS_FIRST_ID, BIG_ROWS, deleted);
        return expected;
    }

    /** Rows were written in id order into one file, so position == id - firstId. */
    private static Set<Integer> positionsAsIds(int firstId) {
        Set<Integer> ids = new HashSet<>();
        for (long pos : DELETED_POSITIONS) {
            ids.add(firstId + (int) pos);
        }
        return ids;
    }

    private static void addRange(List<Integer> into, int firstId, int rows, Set<Integer> except) {
        for (int i = 0; i < rows; i++) {
            if (!except.contains(firstId + i)) {
                into.add(firstId + i);
            }
        }
    }

    /**
     * Every id at or above {@link #RESIDUAL_MIN_ID} present exactly once, no id twice, and fewer rows than the
     * unfiltered total, or the residual skipped nothing.
     */
    private static void assertResidualRead(List<Integer> ids, int unfilteredRows) {
        Assert.assertEquals("no row read twice", ids.size(), new HashSet<>(ids).size());
        Assert.assertTrue("the residual must skip some row groups", ids.size() < unfilteredRows);
        Set<Integer> got = new HashSet<>(ids);
        for (int id = RESIDUAL_MIN_ID; id < BIG_ROWS; id++) {
            Assert.assertTrue("matching row " + id + " lost", got.contains(id));
        }
    }

    // ---- helpers ----

    private static List<FileScanTask> planned(Table table) throws IOException {
        return planned(table.newScan());
    }

    private static List<FileScanTask> planned(TableScan scan) throws IOException {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
            planned.forEach(tasks::add);
        }
        return tasks;
    }

    private static Map<String, List<FileScanTask>> groupByFile(List<FileScanTask> tasks) {
        Map<String, List<FileScanTask>> byPath = new HashMap<>();
        for (FileScanTask task : tasks) {
            byPath.computeIfAbsent(task.file().location(), k -> new ArrayList<>()).add(task);
        }
        return byPath;
    }

    /** Where the first split of a task's file begins: its first recorded row-group offset, or 0 without offsets. */
    private static long firstSplitStart(FileScanTask task) {
        List<Long> offsets = task.file().splitOffsets();
        return offsets == null || offsets.isEmpty() ? 0 : offsets.get(0);
    }

    private static InputFile input(Table table, FileScanTask task) {
        return table.io().newInputFile(task.file().location());
    }

    /** Reads a task exactly as the record reader's standard path does. */
    static List<Integer> readIds(Table table, FileScanTask task, Schema projected) throws IOException {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergFileRecordReader.openStandardRead(table.io(), input(table, task),
                task, table.schema(), projected)) {
            for (Record row : rows) {
                ids.add((Integer) row.getField("id"));
            }
        }
        return ids;
    }

    /** id → variant_field.bucket for every row read. */
    private static Map<Integer, Integer> readBuckets(CloseableIterable<Record> rows) throws IOException {
        Map<Integer, Integer> buckets = new HashMap<>();
        try (rows) {
            for (Record row : rows) {
                Variant variant = (Variant) row.getField(VARIANT_COLUMN);
                Integer bucket = (Integer) variant.value().asObject().get("bucket").asPrimitive().get();
                Assert.assertNull("duplicate row", buckets.put((Integer) row.getField("id"), bucket));
            }
        }
        return buckets;
    }

    /** A task's identity for comparisons: file, range and delete count. */
    private static List<String> identities(List<FileScanTask> tasks) {
        return tasks.stream().map(t -> new File(t.file().location()).getName() + "@" + t.start() + "+" + t.length()
                + "/" + (t.deletes() == null ? 0 : t.deletes().size())).sorted().collect(Collectors.toList());
    }
}
