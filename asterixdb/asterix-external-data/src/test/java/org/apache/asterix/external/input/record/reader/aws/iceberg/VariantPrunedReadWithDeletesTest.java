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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.external.util.iceberg.RequestedVariantPaths;
import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * The variant-pruned read composed with position deletes, on real Parquet files.
 * <p>
 * This is the half of the delete work whose failure mode is a <b>silent wrong answer</b>: an off-by-one in the row
 * position drops a live row or returns a deleted one, and the query still succeeds. So every case here asserts the
 * surviving rows <em>by identity</em> — the actual {@code id} values — and never by count. A count assertion passes
 * whenever the deletion set is dense enough for two errors to cancel, which is exactly the bug being guarded against.
 * <p>
 * The axes that matter are the ones the position arithmetic depends on, and each needs a file spanning several row
 * groups to exercise at all:
 * <ul>
 * <li><b>which row group a deleted position falls in</b> — first, middle, last, and spanning a boundary;
 * <li><b>a split that does not start at row 0</b> — positions are file-absolute, but the reader only sees the row
 * groups in its range, so a reader that counted rows itself would be short by every row group the split dropped;
 * <li><b>delete density</b> — including an entire row group deleted, which must not terminate the read early.
 * </ul>
 * A single-row-group fixture exercises none of this, which is why {@link #ROW_COUNT} and the row-group size below are
 * chosen to produce several.
 */
public class VariantPrunedReadWithDeletesTest {

    private static final String COLUMN = "variant_field";
    private static final int ROW_COUNT = 400;
    /** Small enough that the writer flushes repeatedly; the tests assert the file really spans several row groups. */
    private static final String ROW_GROUP_SIZE_BYTES = "8192";

    /** A test-local index, so the deleted set is explicit and readable rather than hidden in a bitmap. */
    private static final class ExplicitPositionDeleteIndex implements PositionDeleteIndex {
        private final Set<Long> deleted = new HashSet<>();

        @Override
        public void delete(long position) {
            deleted.add(position);
        }

        @Override
        public void delete(long posStart, long posEnd) {
            for (long p = posStart; p < posEnd; p++) {
                deleted.add(p);
            }
        }

        @Override
        public boolean isDeleted(long position) {
            return deleted.contains(position);
        }

        @Override
        public boolean isEmpty() {
            return deleted.isEmpty();
        }
    }

    private static PositionDeleteIndex deletions(long... positions) {
        ExplicitPositionDeleteIndex index = new ExplicitPositionDeleteIndex();
        for (long position : positions) {
            index.delete(position);
        }
        return index;
    }

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, COLUMN, Types.VariantType.get()));
    }

    /** {@code { x: <id>, big: "<padding>" }} — {@code big} is the sub-column pruning must leave unread. */
    private static Variant row(VariantMetadata meta, int id) {
        org.apache.iceberg.variants.ShreddedObject object = Variants.object(meta);
        object.put("x", Variants.of(id));
        object.put("big", Variants.of(("padding-" + id).repeat(20)));
        return Variant.of(meta, object);
    }

    private File writeFixture() throws Exception {
        Schema schema = schema();
        VariantMetadata meta = Variants.metadata("x", "big");
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, row(meta, 0).value());

        File dir = java.nio.file.Files.createTempDirectory("variant-pruned-deletes").toFile();
        File dataFile = new File(dir, "data.parquet");
        GenericRecord template = GenericRecord.create(schema);
        try (FileAppender<Record> writer = Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fieldId, name) -> typedValue)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, ROW_GROUP_SIZE_BYTES).build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                Record record = template.copy();
                record.setField("id", i);
                record.setField(COLUMN, row(meta, i));
                writer.add(record);
            }
        }
        return dataFile;
    }

    /** A plan requesting only {@code variant_field.x}, so {@code big} is pruned away. */
    private static VariantProjectionPlan narrowingPlan() throws Exception {
        ARecordType projected = ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of(COLUMN, "x")));
        VariantProjectionPlan plan = VariantProjectionPlan.from(schema(), projected, true);
        Assert.assertFalse("the plan must actually narrow the variant", plan.isEmpty());
        RequestedVariantPaths paths = plan.get(List.of(COLUMN));
        Assert.assertNotNull("the variant column must be in the plan", paths);
        return plan;
    }

    /** Reads the whole file with the given deletions applied, returning the surviving ids in order. */
    private List<Integer> readIds(File dataFile, PositionDeleteIndex deletions, long splitStart, long splitLength)
            throws Exception {
        return readIds(dataFile, deletions, splitStart, splitLength, null);
    }

    private List<Integer> readIds(File dataFile, PositionDeleteIndex deletions, long splitStart, long splitLength,
            org.apache.iceberg.expressions.Expression filter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(org.apache.iceberg.Files.localInput(dataFile), schema(), filter,
                        splitStart, splitLength, true, narrowingPlan(), deletions)) {
            Assert.assertTrue("the read under test must actually be a pruned read, or this proves nothing",
                    reader.canPrune());
            for (Record record : reader) {
                ids.add((Integer) record.getField("id"));
            }
        }
        return ids;
    }

    private static List<Integer> allIdsExcept(long... deleted) {
        Set<Long> removed = new LinkedHashSet<>();
        for (long d : deleted) {
            removed.add(d);
        }
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            if (!removed.contains((long) i)) {
                expected.add(i);
            }
        }
        return expected;
    }

    private int rowGroupCount(File dataFile) throws Exception {
        try (org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                new org.apache.parquet.io.LocalInputFile(dataFile.toPath()),
                org.apache.parquet.ParquetReadOptions.builder().build())) {
            return reader.getRowGroups().size();
        }
    }

    @Test
    public void fixtureSpansSeveralRowGroups() throws Exception {
        File dataFile = writeFixture();
        int rowGroups = rowGroupCount(dataFile);
        Assert.assertTrue("the fixture must span several row groups, else none of these tests exercise the position "
                + "arithmetic at all; got " + rowGroups, rowGroups >= 3);
    }

    @Test
    public void noDeletions_returnsEveryRow() throws Exception {
        File dataFile = writeFixture();
        Assert.assertEquals(allIdsExcept(), readIds(dataFile, deletions(), 0, 0));
    }

    @Test
    public void deletionsScatteredAcrossRowGroups_dropExactlyThoseRows() throws Exception {
        File dataFile = writeFixture();
        long[] deleted = { 0, 1, 57, 199, 200, 201, ROW_COUNT - 1 };
        Assert.assertEquals(allIdsExcept(deleted), readIds(dataFile, deletions(deleted), 0, 0));
    }

    /**
     * A whole row group deleted. The read must continue past it rather than stopping — a look-ahead that treated
     * "no live row in this row group" as end-of-input would silently truncate the result.
     */
    @Test
    public void anEntireRowGroupDeleted_doesNotTruncateTheRead() throws Exception {
        File dataFile = writeFixture();
        List<Long> deleted = new ArrayList<>();
        for (long p = 0; p < 150; p++) {
            deleted.add(p);
        }
        long[] positions = deleted.stream().mapToLong(Long::longValue).toArray();
        List<Integer> ids = readIds(dataFile, deletions(positions), 0, 0);
        Assert.assertEquals(allIdsExcept(positions), ids);
        Assert.assertEquals("the first surviving row must be the one just past the deleted range", Integer.valueOf(150),
                ids.get(0));
    }

    @Test
    public void everyRowDeleted_returnsNothing() throws Exception {
        File dataFile = writeFixture();
        long[] positions = new long[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            positions[i] = i;
        }
        Assert.assertEquals(List.of(), readIds(dataFile, deletions(positions), 0, 0));
    }

    /**
     * The case the whole design turns on: a split that does not begin at row 0.
     * <p>
     * Positions in a deletion vector are file-absolute. The reader sees only the row groups inside its range, so any
     * scheme that counted rows as it read them would be short by exactly the rows in the dropped row groups, and would
     * delete the wrong rows — silently. Here the split covers the tail of the file and the deleted positions are
     * expressed in file coordinates; if they were interpreted relative to the split, different rows would vanish.
     */
    @Test
    public void splitNotStartingAtRowZero_usesFileAbsolutePositions() throws Exception {
        File dataFile = writeFixture();

        long firstRowGroupEnd;
        long firstRowGroupRows;
        long fileLength = java.nio.file.Files.size(dataFile.toPath());
        try (org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                new org.apache.parquet.io.LocalInputFile(dataFile.toPath()),
                org.apache.parquet.ParquetReadOptions.builder().build())) {
            org.apache.parquet.hadoop.metadata.BlockMetaData first = reader.getRowGroups().get(0);
            firstRowGroupEnd = first.getStartingPos() + first.getCompressedSize();
            firstRowGroupRows = first.getRowCount();
        }

        // Read the tail of the file with no deletions, to learn which ids this split covers.
        List<Integer> splitIds = readIds(dataFile, deletions(), firstRowGroupEnd, fileLength - firstRowGroupEnd);
        Assert.assertFalse("the split must actually drop rows, or this test is vacuous", splitIds.isEmpty());
        Assert.assertEquals("the split must begin after the first row group", Integer.valueOf((int) firstRowGroupRows),
                splitIds.get(0));

        // Now delete three of them, named by their FILE-absolute positions.
        int firstInSplit = splitIds.get(0);
        long[] deleted = { firstInSplit, firstInSplit + 5, splitIds.get(splitIds.size() - 1) };
        List<Integer> expected = new ArrayList<>(splitIds);
        for (long d : deleted) {
            expected.remove(Integer.valueOf((int) d));
        }

        List<Integer> actual = readIds(dataFile, deletions(deleted), firstRowGroupEnd, fileLength - firstRowGroupEnd);
        Assert.assertEquals(expected, actual);

        // Mutation guard: had the positions been treated as split-relative, position `firstInSplit` would have named
        // a row in a row group this split never reads, so the first row of the split would have SURVIVED.
        Assert.assertFalse("a split-relative reading of the positions would have left this row in place",
                actual.contains(firstInSplit));
    }

    /**
     * The same deletions must produce the same answer whether or not the variant is pruned. This is the differential
     * oracle the flag exists for, run against the reader directly: pruned read with deletions versus unpruned read
     * with the identical deletions applied by hand.
     */
    @Test
    public void prunedAndUnprunedAgreeOnWhichRowsSurvive() throws Exception {
        File dataFile = writeFixture();
        long[] deleted = { 3, 4, 5, 120, 121, 333 };

        List<Integer> pruned = readIds(dataFile, deletions(deleted), 0, 0);

        List<Integer> unpruned = new ArrayList<>();
        Set<Long> removed = new HashSet<>();
        for (long d : deleted) {
            removed.add(d);
        }
        long position = 0;
        try (org.apache.iceberg.io.CloseableIterable<Record> it =
                Parquet.read(org.apache.iceberg.Files.localInput(dataFile)).project(schema())
                        .createReaderFunc(
                                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(schema(), fs))
                        .build()) {
            for (Record record : it) {
                if (!removed.contains(position)) {
                    unpruned.add((Integer) record.getField("id"));
                }
                position++;
            }
        }

        Assert.assertEquals("pruned and unpruned reads must agree row for row", unpruned, pruned);
        Assert.assertEquals(allIdsExcept(deleted), pruned);
    }

    /** Pruning must still actually prune when deletions are in play — otherwise the composition bought nothing. */
    @Test
    public void thePrunedReadStillOmitsTheUnreferencedSubColumn() throws Exception {
        File dataFile = writeFixture();
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(org.apache.iceberg.Files.localInput(dataFile), schema(), null, 0, 0,
                        true, narrowingPlan(), deletions(7))) {
            Assert.assertTrue(reader.canPrune());
            int seen = 0;
            for (Record record : reader) {
                org.apache.iceberg.variants.VariantValue value = ((Variant) record.getField(COLUMN)).value();
                Assert.assertNotNull("the requested sub-field must survive", value.asObject().get("x"));
                Assert.assertNull("the unreferenced sub-field must not have been read", value.asObject().get("big"));
                seen++;
            }
            Assert.assertEquals(ROW_COUNT - 1, seen);
        }
    }

    /**
     * Row-group skipping and position deletes together — the combination none of the cases above reaches, because they
     * all pass a null filter and so leave {@code shouldSkip[]} entirely false.
     * <p>
     * This is the arithmetic that is easiest to get wrong and hardest to notice. When the metrics filter skips row
     * groups, the reader advances past them without reading a row, so the count of rows it has read no longer tracks
     * position in the file at all — and the deleted positions are file-absolute. A reader that indexed its row-group
     * table by "how many row groups have I read" rather than "which row group is this" would be off by exactly the
     * skipped groups, and would delete the wrong rows while still returning a plausible-looking answer.
     * <p>
     * The expected set is derived independently: ids are written in ascending order, so row group <i>i</i> holds a
     * known contiguous id range, and the filter keeps precisely those row groups whose range reaches the bound.
     */
    @Test
    public void skippedRowGroupsWithDeletions_stillUseFileAbsolutePositions() throws Exception {
        File dataFile = writeFixture();
        int bound = 200;
        org.apache.iceberg.expressions.Expression filter =
                org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("id", bound);

        // Independently work out which row groups the filter can keep, and which file rows they cover.
        List<Integer> survivingRows = new ArrayList<>();
        int totalRowGroups;
        int survivingRowGroups = 0;
        try (org.apache.parquet.hadoop.ParquetFileReader r = org.apache.parquet.hadoop.ParquetFileReader.open(
                new org.apache.parquet.io.LocalInputFile(dataFile.toPath()),
                org.apache.parquet.ParquetReadOptions.builder().build())) {
            totalRowGroups = r.getRowGroups().size();
            int first = 0;
            for (org.apache.parquet.hadoop.metadata.BlockMetaData block : r.getRowGroups()) {
                int count = (int) block.getRowCount();
                // ids ascend with position, so this row group holds ids [first, first + count)
                if (first + count - 1 >= bound) {
                    survivingRowGroups++;
                    for (int id = first; id < first + count; id++) {
                        survivingRows.add(id);
                    }
                }
                first += count;
            }
        }
        Assert.assertTrue("the filter must actually skip at least one row group, or this test is vacuous",
                survivingRowGroups < totalRowGroups);
        Assert.assertFalse("some row groups must survive", survivingRows.isEmpty());

        // Sanity: with no deletions the filtered read returns exactly the surviving row groups' rows.
        Assert.assertEquals("row-group skipping alone must be unaffected by the deletes plumbing", survivingRows,
                readIds(dataFile, deletions(), 0, 0, filter));

        // Now delete rows inside the surviving range, named by FILE-absolute position. The first of them is the one
        // that a "rows I have read so far" scheme would mislocate, since every skipped row group is uncounted.
        int firstSurviving = survivingRows.get(0);
        long[] deleted = { firstSurviving, firstSurviving + 1, survivingRows.get(survivingRows.size() - 1) };
        List<Integer> expected = new ArrayList<>(survivingRows);
        for (long d : deleted) {
            expected.remove(Integer.valueOf((int) d));
        }

        List<Integer> actual = readIds(dataFile, deletions(deleted), 0, 0, filter);
        Assert.assertEquals(expected, actual);
        Assert.assertFalse("the first surviving row was deleted and must not come back",
                actual.contains(firstSurviving));
    }

    /**
     * An exhaustive sweep rather than a handful of chosen positions: every single row of the file deleted on its own,
     * asserting each time that exactly that row disappears and every other row survives.
     * <p>
     * Chosen positions can all miss the same boundary. This cannot — it puts a deletion at every row-group boundary,
     * at every first and last row of every row group, and everywhere between, by construction.
     */
    @Test
    public void everySinglePositionDeletedInTurn_removesExactlyThatRow() throws Exception {
        File dataFile = writeFixture();
        for (int position = 0; position < ROW_COUNT; position++) {
            List<Integer> ids = readIds(dataFile, deletions(position), 0, 0);
            Assert.assertEquals("deleting position " + position + " must remove exactly one row", ROW_COUNT - 1,
                    ids.size());
            Assert.assertFalse("row " + position + " must be gone", ids.contains(position));
            Assert.assertEquals("and every other row must be intact, in order", allIdsExcept(position), ids);
        }
    }

    /**
     * A vector whose positions all lie in row groups this split never reads. Nothing in the split may be removed, and
     * — the part that could actually go wrong — the reader must not terminate early or mis-account rows just because
     * the bitmap is non-empty while none of its positions are ever encountered.
     */
    @Test
    public void deletionsEntirelyOutsideTheSplit_removeNothingFromIt() throws Exception {
        File dataFile = writeFixture();
        long firstRowGroupEnd;
        long firstRowGroupRows;
        long fileLength = java.nio.file.Files.size(dataFile.toPath());
        try (org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                new org.apache.parquet.io.LocalInputFile(dataFile.toPath()),
                org.apache.parquet.ParquetReadOptions.builder().build())) {
            org.apache.parquet.hadoop.metadata.BlockMetaData first = reader.getRowGroups().get(0);
            firstRowGroupEnd = first.getStartingPos() + first.getCompressedSize();
            firstRowGroupRows = first.getRowCount();
        }
        // Every deleted position is inside the FIRST row group, which the split below excludes.
        long[] deleted = new long[(int) firstRowGroupRows];
        for (int i = 0; i < deleted.length; i++) {
            deleted[i] = i;
        }
        List<Integer> withoutDeletes = readIds(dataFile, deletions(), firstRowGroupEnd, fileLength - firstRowGroupEnd);
        List<Integer> withDeletes =
                readIds(dataFile, deletions(deleted), firstRowGroupEnd, fileLength - firstRowGroupEnd);
        Assert.assertFalse(withoutDeletes.isEmpty());
        Assert.assertEquals("positions outside the split must change nothing inside it", withoutDeletes, withDeletes);
        Assert.assertEquals("the split must start right after the deleted row group",
                Integer.valueOf((int) firstRowGroupRows), withDeletes.get(0));
    }

    /**
     * A seeded randomized sweep over the combinations the chosen cases cannot enumerate: random deletion density
     * (sparse, moderate, dense), a random split boundary (whole file, or starting at a random row group), and a
     * residual filter that skips leading row groups, all at once, each iteration checked against a brute-force
     * expectation computed from the footer.
     * <p>
     * The seed is fixed and printed with every failure, so a red run is reproducible rather than a flake report.
     */
    @Test
    public void randomizedDeletionsSplitsAndSkips_matchBruteForce() throws Exception {
        long seed = 20260920L;
        java.util.Random random = new java.util.Random(seed);
        File dataFile = writeFixture();

        // Row-group layout from the footer: [firstRow, rowCount, byteStart, byteEnd] per group, in file order.
        List<long[]> groups = new ArrayList<>();
        long fileLength = java.nio.file.Files.size(dataFile.toPath());
        try (org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                new org.apache.parquet.io.LocalInputFile(dataFile.toPath()),
                org.apache.parquet.ParquetReadOptions.builder().build())) {
            long first = 0;
            for (org.apache.parquet.hadoop.metadata.BlockMetaData block : reader.getRowGroups()) {
                groups.add(new long[] { first, block.getRowCount(), block.getStartingPos(),
                        block.getStartingPos() + block.getCompressedSize() });
                first += block.getRowCount();
            }
        }
        Assert.assertTrue(groups.size() >= 3);

        double[] densities = { 0.05, 0.3, 0.7 };
        int iterations = 30;
        for (int iteration = 0; iteration < iterations; iteration++) {
            String where = "seed=" + seed + " iteration=" + iteration;
            double density = densities[random.nextInt(densities.length)];
            Set<Long> deleted = new HashSet<>();
            for (long p = 0; p < ROW_COUNT; p++) {
                if (random.nextDouble() < density) {
                    deleted.add(p);
                }
            }

            // Split: either the whole file, or from the byte start of a random row group to the end.
            int firstGroupInSplit = random.nextBoolean() ? 0 : random.nextInt(groups.size());
            long splitStart = firstGroupInSplit == 0 ? 0 : groups.get(firstGroupInSplit)[2];
            long splitLength = firstGroupInSplit == 0 ? 0 : fileLength - splitStart;

            // Filter: sometimes none, sometimes "id >= bound" with bound at a random row-group boundary, which
            // skips every row group entirely below it.
            int boundGroup = random.nextInt(groups.size());
            boolean useFilter = random.nextBoolean();
            long bound = groups.get(boundGroup)[0];
            org.apache.iceberg.expressions.Expression filter =
                    useFilter ? org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("id", (int) bound) : null;

            // Brute force: a row survives iff its group is in the split, its group is not skipped, and it is not deleted.
            List<Integer> expected = new ArrayList<>();
            for (int g = firstGroupInSplit; g < groups.size(); g++) {
                long[] group = groups.get(g);
                long lastId = group[0] + group[1] - 1;
                if (useFilter && lastId < bound) {
                    continue; // the metrics filter skips this whole row group
                }
                for (long id = group[0]; id < group[0] + group[1]; id++) {
                    if (!deleted.contains(id)) {
                        expected.add((int) id);
                    }
                }
            }

            long[] positions = deleted.stream().mapToLong(Long::longValue).toArray();
            List<Integer> actual = readIds(dataFile, deletions(positions), splitStart, splitLength, filter);
            Assert.assertEquals(where + " density=" + density + " splitFromGroup=" + firstGroupInSplit + " filter="
                    + (useFilter ? ">= " + bound : "none"), expected, actual);
        }
    }

    /**
     * The two-phase open's safety catch: opened for position deletes but never given them, the reader must refuse to
     * iterate. Without this, a caller that declined-then-forgot would silently return every deleted row.
     */
    @Test
    public void openedForPositionDeletesWithoutSupplyingThem_refusesToIterate() throws Exception {
        File dataFile = writeFixture();
        try (VariantProjectedParquetReader reader = VariantProjectedParquetReader.open(
                org.apache.iceberg.Files.localInput(dataFile), schema(), null, 0, 0, true, narrowingPlan(), true)) {
            Assert.assertTrue(reader.canPrune());
            try {
                reader.iterator();
                Assert.fail("iterating without positions must be refused");
            } catch (IllegalStateException expected) {
                // the guard
            }
            // and it must not be possible to attach positions to a reader that was not opened for them
            try (VariantProjectedParquetReader plain =
                    VariantProjectedParquetReader.open(org.apache.iceberg.Files.localInput(dataFile), schema(), null, 0,
                            0, true, narrowingPlan(), false)) {
                try {
                    plain.withDeletedPositions(deletions(1));
                    Assert.fail("attaching positions to a reader not opened for them must be refused");
                } catch (IllegalStateException expected) {
                    // the guard
                }
            }
        }
    }
}
