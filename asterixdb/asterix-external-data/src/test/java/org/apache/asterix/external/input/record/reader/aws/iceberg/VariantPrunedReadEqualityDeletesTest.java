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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * The pruned delete path driven through a <b>real Iceberg table</b>, with real delete files planned by Iceberg itself.
 * <p>
 * Its sibling {@code VariantPrunedReadWithDeletesTest} hands the reader a deletion bitmap directly, which is the right
 * shape for exercising the position arithmetic exhaustively but stops short of the machinery that produces one. This
 * test starts from a committed table and a {@link FileScanTask} planned by Iceberg, so it covers the parts that a
 * hand-built index cannot:
 * <ul>
 * <li><b>equality deletes at all</b> — the reader is opened against {@code deleteFilter.requiredSchema()}, and the
 * keep-predicate matches rows through a {@code StructProjection} built over that same schema. Nothing about that is
 * exercised by passing a bitmap, and a mismatch between the schema the records are materialized against and the one
 * the projection expects would misread the equality key;
 * <li><b>the widened required schema still clipping</b> — an equality delete adds its key columns to the read, and the
 * clip must still land on the variant, which it does because the plan is keyed by column path rather than position;
 * <li><b>deletion vectors through Iceberg's own delete loader</b> — reaching the reader as a bitmap only after the
 * Puffin blob has been read and merged, rather than being handed in ready-made;
 * <li><b>both kinds on one task</b> — position deletes skipped inside the reader while equality deletes filter over
 * it, the one arrangement where the two mechanisms have to agree about which rows remain.
 * </ul>
 * Every case asserts the surviving ids against Iceberg's own answer for the same table, computed with the pruned path
 * disabled — a differential oracle rather than a hand-written expectation, so the test cannot encode the same mistake
 * twice.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Drives the pruned delete path from a committed Iceberg table so equality deletes, v2 position delete "
        + "files and the both-kinds-at-once case are covered against Iceberg's own delete filter as oracle")
public class VariantPrunedReadEqualityDeletesTest {

    private static final String COLUMN = "variant_field";
    private static final int ROW_COUNT = 300;

    /** The table's full schema. {@code bucket} exists only to be an equality-delete key. */
    private static final Schema TABLE_SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
            Types.NestedField.optional(3, COLUMN, Types.VariantType.get()));

    /**
     * What the query projects — deliberately WITHOUT {@code bucket}.
     * <p>
     * This is what makes the equality-delete cases mean anything. When the delete key is a column the query already
     * selects, {@code requiredSchema} equals the projection and the whole widening question never arises: opening the
     * pruned reader against the wrong one of the two is then indistinguishable. Leaving {@code bucket} out forces
     * {@code requiredSchema} to be strictly wider, so a reader opened against the projection materializes records
     * without the equality key and the delete predicate cannot match.
     */
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, COLUMN, Types.VariantType.get()));

    private static Variant variant(VariantMetadata meta, int id) {
        ShreddedObject object = Variants.object(meta);
        object.put("x", Variants.of(id));
        object.put("big", Variants.of(("padding-" + id).repeat(20)));
        return Variant.of(meta, object);
    }

    /** A table holding one data file of {@link #ROW_COUNT} rows, shredded, spanning several row groups. */
    private Table createTable(File warehouse) throws Exception {
        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "3");
        properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "8192");
        Table table = tables.create(TABLE_SCHEMA, PartitionSpec.unpartitioned(), properties, warehouse.toString());

        VariantMetadata meta = Variants.metadata("x", "big");
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, variant(meta, 0).value());

        File dataFile = new File(warehouse, "data/rows.parquet");
        Assert.assertTrue("warehouse data dir", dataFile.getParentFile().mkdirs() || dataFile.getParentFile().exists());
        GenericRecord template = GenericRecord.create(TABLE_SCHEMA);
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(dataFile))
                .schema(TABLE_SCHEMA).createWriterFunc(GenericParquetWriter::create)
                .variantShreddingFunc((fieldId, name) -> typedValue)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "8192").build();
        try (FileAppender<Record> writer = appender) {
            for (int i = 0; i < ROW_COUNT; i++) {
                Record record = template.copy();
                record.setField("id", i);
                record.setField("bucket", i % 10);
                record.setField(COLUMN, variant(meta, i));
                writer.add(record);
            }
        }
        DataFile committed = org.apache.iceberg.DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(org.apache.iceberg.Files.localInput(dataFile)).withMetrics(appender.metrics())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET).build();
        table.newAppend().appendFile(committed).commit();
        return table;
    }

    /** An equality delete on {@code bucket}, removing every row whose bucket is one of {@code buckets}. */
    private DeleteFile writeEqualityDelete(Table table, File warehouse, String name, int... buckets) throws Exception {
        Schema deleteSchema = table.schema().select("bucket");
        File file = new File(warehouse, "data/" + name + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(table.schema().findField("bucket").fieldId())).overwrite()
                .buildEqualityWriter();
        GenericRecord template = GenericRecord.create(deleteSchema);
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int bucket : buckets) {
                Record row = template.copy();
                row.setField("bucket", bucket);
                open.write(row);
            }
        }
        return writer.toDeleteFile();
    }

    /**
     * A deletion vector removing the given file-absolute positions.
     * <p>
     * Not a v2 position delete <em>file</em>. A VARIANT column requires table format v3 — Iceberg rejects the schema
     * below it with "variant is not supported until v3" (measured) — and v3's mechanism for row positions is the
     * deletion vector, which is what this writes.
     * <p>
     * Whether a v3 table could still be made to carry an old-style position delete file was <b>not</b> established
     * here: an attempt failed inside the {@code PositionDeleteWriter} fixture, not at commit, so that says nothing
     * about what Iceberg permits. It does not matter for the code under test either way — both forms arrive as the
     * same merged {@link org.apache.iceberg.deletes.PositionDeleteIndex} from {@code deletedRowPositions()}, so the
     * reader cannot tell them apart.
     */
    private DeleteFile writeDeletionVector(Table table, String dataFilePath, String name, long... positions)
            throws Exception {
        String path = table.location() + "/deletes/dv-" + name + ".puffin";
        DVFileWriter writer = new BaseDVFileWriter(() -> table.io().newOutputFile(path), ignored -> null);
        try (DVFileWriter open = writer) {
            for (long position : positions) {
                open.delete(dataFilePath, position, table.spec(), null);
            }
        }
        List<DeleteFile> written = writer.result().deleteFiles();
        Assert.assertEquals("v3 allows at most one deletion vector per data file", 1, written.size());
        return written.get(0);
    }

    private static VariantProjectionPlan narrowingPlan() throws Exception {
        ARecordType projected = ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of(COLUMN, "x")));
        VariantProjectionPlan plan = VariantProjectionPlan.from(SCHEMA, projected, true);
        Assert.assertFalse("the plan must narrow the variant", plan.isEmpty());
        return plan;
    }

    private static FileScanTask singleTask(Table table) {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        Assert.assertEquals("fixture must plan to exactly one task", 1, tasks.size());
        return tasks.get(0);
    }

    /** Iceberg's own answer: the standard delete-aware read, with no variant pruning anywhere. */
    private List<Integer> icebergOracle(Table table, FileScanTask task) throws Exception {
        return icebergOracle(table, task, table.io().newInputFile(task.file().location()), SCHEMA);
    }

    private List<Integer> icebergOracle(Table table, FileScanTask task, org.apache.iceberg.io.InputFile dataFile,
            Schema projection) throws Exception {
        org.apache.iceberg.data.GenericDeleteFilter deleteFilter =
                new org.apache.iceberg.data.GenericDeleteFilter(table.io(), task, table.schema(), projection);
        Schema required = deleteFilter.requiredSchema();
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows =
                Parquet.read(dataFile).project(required).filter(task.residual()).split(task.start(), task.length())
                        .createReaderFunc(
                                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(required, fs))
                        .build()) {
            for (Record record : deleteFilter.filter(rows)) {
                ids.add((Integer) record.getField("id"));
            }
        }
        return ids;
    }

    /**
     * The path under test — the <b>shipped</b> composition, not a copy of it. This calls the exact method
     * {@code IcebergFileRecordReader} uses for a delete-bearing task, so a change to the production routing that this
     * test does not follow will break it rather than pass silently.
     */
    private List<Integer> prunedWithDeletes(Table table, FileScanTask task) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> live = IcebergFileRecordReader.openPrunedDeleteAwareRead(table.io(),
                table.io().newInputFile(task.file().location()), task, table.schema(), SCHEMA, narrowingPlan())) {
            Assert.assertNotNull("the shipped code must have taken the pruned path, or this proves nothing", live);
            for (Record record : live) {
                ids.add((Integer) record.getField("id"));
                // The whole point of the pruned read: the unreferenced sub-column must not have been materialized.
                Assert.assertNull("the unreferenced sub-field must not be read",
                        ((Variant) record.getField(COLUMN)).value().asObject().get("big"));
            }
        }
        return ids;
    }

    private File warehouse() throws Exception {
        return java.nio.file.Files.createTempDirectory("variant-eq-deletes").resolve("tbl").toFile();
    }

    @Test
    public void equalityDeletes_matchIcebergRowForRow() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq", 2, 7)).commit();

        FileScanTask task = singleTask(table);
        Assert.assertFalse("the task must carry deletes", task.deletes().isEmpty());

        List<Integer> oracle = icebergOracle(table, task);
        List<Integer> actual = prunedWithDeletes(table, task);

        Assert.assertEquals("pruned equality-delete read must match Iceberg row for row", oracle, actual);
        Assert.assertEquals("two buckets of ten removed", ROW_COUNT - (ROW_COUNT / 10) * 2, actual.size());
        for (int id : actual) {
            Assert.assertNotEquals("bucket 2 must be gone", 2, id % 10);
            Assert.assertNotEquals("bucket 7 must be gone", 7, id % 10);
        }
    }

    @Test
    public void deletionVector_matchesIcebergRowForRow() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        String path = singleTask(table).file().location();
        table.newRowDelta().addDeletes(writeDeletionVector(table, path, "pos", 0, 1, 99, 150, ROW_COUNT - 1)).commit();

        FileScanTask task = singleTask(table);
        List<Integer> oracle = icebergOracle(table, task);
        List<Integer> actual = prunedWithDeletes(table, task);

        Assert.assertEquals("pruned deletion-vector read must match Iceberg row for row", oracle, actual);
        Assert.assertEquals(ROW_COUNT - 5, actual.size());
        for (int gone : new int[] { 0, 1, 99, 150, ROW_COUNT - 1 }) {
            Assert.assertFalse("row " + gone + " must be gone", actual.contains(gone));
        }
    }

    /**
     * Both kinds on one task. Position deletes are skipped inside the reader and equality deletes filter over it, so
     * this is the only case where the two mechanisms must agree about the same row set — and the overlap (a position
     * delete naming a row an equality delete also removes) must not double-count or resurrect anything.
     */
    @Test
    public void positionAndEqualityDeletesTogether_matchIcebergRowForRow() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        String path = singleTask(table).file().location();
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq", 4))
                .addDeletes(writeDeletionVector(table, path, "pos", 0, 4, 5, 200)).commit();

        FileScanTask task = singleTask(table);
        Assert.assertEquals("both delete files must reach the task", 2, task.deletes().size());

        List<Integer> oracle = icebergOracle(table, task);
        List<Integer> actual = prunedWithDeletes(table, task);

        Assert.assertEquals("pruned mixed-delete read must match Iceberg row for row", oracle, actual);
        // Row 4 is removed twice over (bucket 4, and position 4); it must simply be absent, once.
        Assert.assertFalse(actual.contains(4));
        Assert.assertFalse(actual.contains(0));
        Assert.assertFalse(actual.contains(5));
        Assert.assertFalse(actual.contains(200));
    }

    /** No deletes at all through the same composition path — the degenerate case must not lose rows. */
    @Test
    public void noDeletes_matchesIcebergRowForRow() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        FileScanTask task = singleTask(table);
        Assert.assertTrue(task.deletes().isEmpty());
        Assert.assertEquals(icebergOracle(table, task), prunedWithDeletes(table, task));
    }

    /**
     * An equality delete adds its key column to {@code requiredSchema}. The clip must still narrow the variant — if
     * widening the schema stopped the plan matching, pruning would silently switch itself off on exactly the tables
     * this work exists to speed up, and every correctness assertion above would still pass.
     */
    @Test
    public void theWidenedRequiredSchemaStillPrunes() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq", 1)).commit();

        FileScanTask task = singleTask(table);
        PositionlessGenericDeleteFilter deleteFilter =
                new PositionlessGenericDeleteFilter(table.io(), task, table.schema(), SCHEMA);
        Assert.assertTrue(
                "the required schema must be STRICTLY wider than the projection, or the equality-delete "
                        + "cases below cannot distinguish the two schemas at all",
                deleteFilter.requiredSchema().columns().size() > SCHEMA.columns().size());
        Assert.assertNotNull("and the widening must be the equality key itself",
                deleteFilter.requiredSchema().findField("bucket"));
        Assert.assertNull("and it must NOT carry the synthetic _pos column",
                deleteFilter.requiredSchema().findField(org.apache.iceberg.MetadataColumns.ROW_POSITION.fieldId()));

        try (VariantProjectedParquetReader reader = VariantProjectedParquetReader.open(
                table.io().newInputFile(task.file().location()), deleteFilter.requiredSchema(), task.residual(),
                task.start(), task.length(), true, narrowingPlan(), null)) {
            Assert.assertTrue("the clip must still land on the variant through the widened schema", reader.canPrune());
        }
    }

    /**
     * Two equality-delete files with <em>different</em> key sets on one task. Iceberg groups equality deletes by their
     * field-id set and ORs the resulting predicates; a composition that only honoured the first group, or that built
     * one projection for both, would keep rows the second file removes.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void twoEqualityDeletesWithDifferentKeys_bothApply() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq-bucket", 3))
                .addDeletes(writeEqualityDeleteOnId(table, warehouse, "eq-id", 10, 11, 250)).commit();

        FileScanTask task = singleTask(table);
        Assert.assertEquals(2, task.deletes().size());

        List<Integer> oracle = icebergOracle(table, task);
        List<Integer> actual = prunedWithDeletes(table, task);
        Assert.assertEquals(oracle, actual);
        for (int id : actual) {
            Assert.assertNotEquals("bucket 3 must be gone", 3, id % 10);
        }
        for (int gone : new int[] { 10, 11, 250 }) {
            Assert.assertFalse("id " + gone + " must be gone", actual.contains(gone));
        }
        // bucket 3 removes 30 rows; 10, 11 and 250 sit in buckets 0, 1 and 0, so they are three further rows
        Assert.assertEquals(ROW_COUNT - ROW_COUNT / 10 - 3, actual.size());
    }

    /**
     * The whole file read as several real splits — planned by Iceberg's own {@link FileScanTask#split(long)} — with
     * both a deletion vector and an equality delete attached. Each split is driven through the shipped composition
     * independently, and the concatenation must equal Iceberg's single whole-file answer.
     * <p>
     * This is the arrangement production actually runs: a delete-bearing file cut into splits whose positions are
     * file-absolute while each reader only sees its own row groups. The direct-bitmap tests cover the arithmetic; this
     * covers that the shipped path threads {@code task.start()}/{@code task.length()} and the file-wide bitmap
     * through correctly, per split, with the equality key column widening the schema at the same time.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void realSplitsWithBothDeleteKinds_unionMatchesWholeFileOracle() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        String path = singleTask(table).file().location();
        long[] dvPositions = { 0, 1, 2, 77, 150, 151, 299 };
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq", 6))
                .addDeletes(writeDeletionVector(table, path, "dv", dvPositions)).commit();

        FileScanTask whole = singleTask(table);
        List<Integer> oracle = icebergOracle(table, whole);

        // Cut the task finely enough that several splits each cover a proper subset of the row groups.
        long splitSize = Math.max(1024, whole.length() / 5);
        List<FileScanTask> splits = new ArrayList<>();
        whole.split(splitSize).forEach(splits::add);
        Assert.assertTrue("the task must actually split into several pieces, got " + splits.size(), splits.size() >= 3);

        List<Integer> union = new ArrayList<>();
        int splitsThatProducedRows = 0;
        for (FileScanTask split : splits) {
            Assert.assertEquals("every split must carry the task's deletes", whole.deletes().size(),
                    split.deletes().size());
            List<Integer> part = prunedWithDeletes(table, split);
            if (!part.isEmpty()) {
                splitsThatProducedRows++;
            }
            union.addAll(part);
        }
        Assert.assertTrue("more than one split must have produced rows, else splitting was not exercised",
                splitsThatProducedRows >= 2);
        Assert.assertEquals("splits together must reproduce the whole-file delete-aware read exactly", oracle, union);
        for (long gone : dvPositions) {
            Assert.assertFalse(union.contains((int) gone));
        }
    }

    /** A dense deletion vector — roughly every third row, in every row group — through the real delete loader. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void denseDeletionVector_matchesIcebergRowForRow() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        String path = singleTask(table).file().location();
        List<Long> positions = new ArrayList<>();
        for (long p = 0; p < ROW_COUNT; p += 3) {
            positions.add(p);
        }
        table.newRowDelta().addDeletes(
                writeDeletionVector(table, path, "dense", positions.stream().mapToLong(Long::longValue).toArray()))
                .commit();

        FileScanTask task = singleTask(table);
        List<Integer> oracle = icebergOracle(table, task);
        List<Integer> actual = prunedWithDeletes(table, task);
        Assert.assertEquals(oracle, actual);
        Assert.assertEquals(ROW_COUNT - positions.size(), actual.size());
        for (int id : actual) {
            Assert.assertNotEquals("every id divisible by three must be gone", 0, id % 3);
        }
    }

    /**
     * Closing what {@code openPrunedDeleteAwareRead} returns must close the Parquet reader underneath — on the
     * equality path it is wrapped in {@code CloseableIterable.filter(..)}, and if that wrapper did not propagate
     * close, every delete-bearing file would leak an open input stream. Asserted rather than assumed, because it rests
     * on an Iceberg implementation detail ({@code combine(.., iterable)}) that a version bump could change.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void closingTheReturnedIterableClosesTheUnderlyingStream() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        table.newRowDelta().addDeletes(writeEqualityDelete(table, warehouse, "eq", 1)).commit();
        FileScanTask task = singleTask(table);

        TrackingInputFile tracked = new TrackingInputFile(table.io().newInputFile(task.file().location()));
        CloseableIterable<Record> live = IcebergFileRecordReader.openPrunedDeleteAwareRead(table.io(), tracked, task,
                table.schema(), SCHEMA, narrowingPlan());
        Assert.assertNotNull(live);
        Assert.assertTrue("opening must have opened at least one stream", tracked.streamsOpened.get() >= 1);
        int consumed = 0;
        for (Record ignored : live) {
            consumed++;
        }
        Assert.assertTrue(consumed > 0);
        Assert.assertTrue("streams are still open before close()", tracked.streamsOpen.get() > 0);
        live.close();
        Assert.assertEquals("every stream opened for the read must be closed by close()", 0, tracked.streamsOpen.get());
    }

    /** An equality delete keyed on {@code id} — a column that IS in the projection, unlike {@code bucket}. */
    private DeleteFile writeEqualityDeleteOnId(Table table, File warehouse, String name, int... ids) throws Exception {
        Schema deleteSchema = table.schema().select("id");
        File file = new File(warehouse, "data/" + name + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file))
                .forTable(table).rowSchema(deleteSchema).createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(Collections.singletonList(table.schema().findField("id").fieldId())).overwrite()
                .buildEqualityWriter();
        GenericRecord template = GenericRecord.create(deleteSchema);
        try (EqualityDeleteWriter<Record> open = writer) {
            for (int id : ids) {
                Record row = template.copy();
                row.setField("id", id);
                open.write(row);
            }
        }
        return writer.toDeleteFile();
    }

    /** Counts streams opened and still open, so a test can assert that close() reaches the file reader. */
    private static final class TrackingInputFile implements org.apache.iceberg.io.InputFile {
        private final org.apache.iceberg.io.InputFile delegate;
        final java.util.concurrent.atomic.AtomicInteger streamsOpened = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger streamsOpen = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicLong bytesRead = new java.util.concurrent.atomic.AtomicLong();

        TrackingInputFile(org.apache.iceberg.io.InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public org.apache.iceberg.io.SeekableInputStream newStream() {
            org.apache.iceberg.io.SeekableInputStream inner = delegate.newStream();
            streamsOpened.incrementAndGet();
            streamsOpen.incrementAndGet();
            return new org.apache.iceberg.io.SeekableInputStream() {
                @Override
                public long getPos() throws java.io.IOException {
                    return inner.getPos();
                }

                @Override
                public void seek(long newPos) throws java.io.IOException {
                    inner.seek(newPos);
                }

                @Override
                public int read() throws java.io.IOException {
                    int b = inner.read();
                    if (b >= 0) {
                        bytesRead.incrementAndGet();
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws java.io.IOException {
                    int n = inner.read(b, off, len);
                    if (n > 0) {
                        bytesRead.addAndGet(n);
                    }
                    return n;
                }

                @Override
                public void close() throws java.io.IOException {
                    inner.close();
                    streamsOpen.decrementAndGet();
                }
            };
        }

        @Override
        public String location() {
            return delegate.location();
        }

        @Override
        public boolean exists() {
            return delegate.exists();
        }
    }

    /**
     * The delete file itself cannot be read. The fail-safe must hold at the <em>load</em>, not just at the clip: the
     * reader is already open when the deletion vector is fetched, so a failure there has to (a) reach the caller as an
     * exception rather than a half-configured reader, (b) emit no row first, and (c) close the reader it opened.
     * <p>
     * (c) is the one that would otherwise go unnoticed — a leak per delete-bearing file per failing query, visible
     * only as exhausted file handles much later.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void unreadableDeletionVector_failsBeforeAnyRowAndClosesTheReader() throws Exception {
        File warehouse = warehouse();
        Table table = createTable(warehouse);
        String path = singleTask(table).file().location();
        table.newRowDelta().addDeletes(writeDeletionVector(table, path, "dv", 1, 2, 3)).commit();
        FileScanTask task = singleTask(table);

        FaultyFileIO faultyIo = new FaultyFileIO(table.io(), ".puffin");
        TrackingInputFile tracked = new TrackingInputFile(table.io().newInputFile(path));

        try {
            IcebergFileRecordReader.openPrunedDeleteAwareRead(faultyIo, tracked, task, table.schema(), SCHEMA,
                    narrowingPlan());
            Assert.fail("loading the deletion vector must fail loudly");
        } catch (RuntimeException | java.io.IOException expected) {
            Assert.assertTrue("the failure must be the injected one, not something incidental: " + expected,
                    String.valueOf(expected.getMessage()).contains(FaultyFileIO.MESSAGE) || expected.getCause() != null
                            && String.valueOf(expected.getCause().getMessage()).contains(FaultyFileIO.MESSAGE));
        }
        Assert.assertTrue("the data file was opened (the reader got as far as the clip)",
                tracked.streamsOpened.get() >= 1);
        Assert.assertEquals("and every stream it opened was closed again on the way out", 0, tracked.streamsOpen.get());
    }

    /** Delegates everything, but refuses to open any file whose location ends with the given suffix. */
    private static final class FaultyFileIO implements org.apache.iceberg.io.FileIO {
        static final String MESSAGE = "injected failure reading delete file";
        private final org.apache.iceberg.io.FileIO delegate;
        private final String failingSuffix;

        FaultyFileIO(org.apache.iceberg.io.FileIO delegate, String failingSuffix) {
            this.delegate = delegate;
            this.failingSuffix = failingSuffix;
        }

        @Override
        public org.apache.iceberg.io.InputFile newInputFile(String location) {
            if (location.endsWith(failingSuffix)) {
                throw new org.apache.iceberg.exceptions.RuntimeIOException(new java.io.IOException(MESSAGE), "%s: %s",
                        MESSAGE, location);
            }
            return delegate.newInputFile(location);
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String location) {
            return delegate.newOutputFile(location);
        }

        @Override
        public void deleteFile(String location) {
            delegate.deleteFile(location);
        }
    }

    private static final int WIDE_ROWS = 20_000;
    private static final int WIDE_FIELDS = 40;

    /**
     * Pruning under deletes, <em>measured</em> rather than inferred.
     * <p>
     * Every other case here proves correctness — the rows come back right — and asserts that the read was pruned by
     * checking {@code canPrune()} and that the unreferenced sub-field is absent from each record. Neither of those
     * says how much was saved, and nothing at the cluster level can tell a pruned read from a silently declined one.
     * This test is the instrument for that: one wide shredded variant (one cheap {@code x} and {@value #WIDE_FIELDS}
     * padded string fields whose content varies per row so no encoding collapses them), a deletion vector attached so
     * the read is the delete-aware one, then the same task read twice through a byte-counting input file — Iceberg's
     * standard delete path against the shipped pruned composition.
     * <p>
     * <b>Bytes are asserted; time is printed.</b> Bytes read for a given file are deterministic, so "pruned reads less
     * than half" cannot flake. Wall-clock is the number a person wants to see and the one that would flake a build
     * agent, so it goes to stdout, as the sibling deep/wide tests in {@code ReadingShreddedPushdownTest} do.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void prunedDeleteAwareRead_readsFarFewerBytesThanTheStandardDeletePath() throws Exception {
        File warehouse = warehouse();
        Table table = createWideTable(warehouse);
        String path = singleTask(table).file().location();
        long[] dv = new long[WIDE_ROWS / 50];
        for (int i = 0; i < dv.length; i++) {
            dv[i] = i * 50L;
        }
        table.newRowDelta().addDeletes(writeDeletionVector(table, path, "dv", dv)).commit();
        FileScanTask task = singleTask(table);
        Assert.assertFalse(task.deletes().isEmpty());

        TrackingInputFile standardFile = new TrackingInputFile(table.io().newInputFile(path));
        long standardStart = System.nanoTime();
        List<Integer> standard = icebergOracle(table, task, standardFile, SCHEMA);
        long standardMillis = (System.nanoTime() - standardStart) / 1_000_000;

        TrackingInputFile prunedFile = new TrackingInputFile(table.io().newInputFile(path));
        long prunedStart = System.nanoTime();
        List<Integer> pruned = new ArrayList<>();
        try (CloseableIterable<Record> live = IcebergFileRecordReader.openPrunedDeleteAwareRead(table.io(), prunedFile,
                task, table.schema(), SCHEMA, narrowingPlan())) {
            Assert.assertNotNull("the shipped code must have taken the pruned path", live);
            for (Record record : live) {
                pruned.add((Integer) record.getField("id"));
            }
        }
        long prunedMillis = (System.nanoTime() - prunedStart) / 1_000_000;

        Assert.assertEquals("both reads must agree on the surviving rows", standard, pruned);
        Assert.assertEquals(WIDE_ROWS - dv.length, pruned.size());

        long standardBytes = standardFile.bytesRead.get();
        long prunedBytes = prunedFile.bytesRead.get();
        // Deliberately stdout: the measurement is the point, and it has to be visible in ordinary build output.
        System.out.printf(
                "wide variant under a deletion vector (%d rows, %d padded fields, %d deleted): standard delete path "
                        + "%d bytes in %d ms, pruned delete path %d bytes in %d ms (%.1f%% of the bytes)%n",
                WIDE_ROWS, WIDE_FIELDS, dv.length, standardBytes, standardMillis, prunedBytes, prunedMillis,
                100.0 * prunedBytes / standardBytes);
        Assert.assertTrue("both reads must have read something", standardBytes > 0 && prunedBytes > 0);
        Assert.assertTrue("the pruned delete-aware read must fetch well under half the bytes of the standard one; "
                + "pruned=" + prunedBytes + " standard=" + standardBytes, prunedBytes * 2 < standardBytes);
    }

    /** {@code { x: <id>, f0: "<varying padding>", ..., f39: ... }} — the padding is what pruning must not fetch. */
    private static Variant wideVariant(VariantMetadata meta, int id) {
        ShreddedObject object = Variants.object(meta);
        object.put("x", Variants.of(id));
        for (int k = 0; k < WIDE_FIELDS; k++) {
            // Varies per row AND per field so neither dictionary nor run-length encoding can shrink it away.
            object.put("f" + k, Variants.of("f" + k + "-" + id + "-" + Integer.toHexString(id * 31 + k).repeat(6)));
        }
        return Variant.of(meta, object);
    }

    private Table createWideTable(File warehouse) throws Exception {
        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "3");
        properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1 << 20));
        Table table = tables.create(TABLE_SCHEMA, PartitionSpec.unpartitioned(), properties, warehouse.toString());

        List<String> names = new ArrayList<>();
        names.add("x");
        for (int k = 0; k < WIDE_FIELDS; k++) {
            names.add("f" + k);
        }
        VariantMetadata meta = Variants.metadata(names.toArray(new String[0]));
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, wideVariant(meta, 0).value());

        File dataFile = new File(warehouse, "data/wide.parquet");
        Assert.assertTrue(dataFile.getParentFile().mkdirs() || dataFile.getParentFile().exists());
        GenericRecord template = GenericRecord.create(TABLE_SCHEMA);
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(dataFile))
                .schema(TABLE_SCHEMA).createWriterFunc(GenericParquetWriter::create)
                .variantShreddingFunc((fieldId, name) -> typedValue)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1 << 20)).build();
        try (FileAppender<Record> writer = appender) {
            for (int i = 0; i < WIDE_ROWS; i++) {
                Record record = template.copy();
                record.setField("id", i);
                record.setField("bucket", i % 10);
                record.setField(COLUMN, wideVariant(meta, i));
                writer.add(record);
            }
        }
        DataFile committed = org.apache.iceberg.DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(org.apache.iceberg.Files.localInput(dataFile)).withMetrics(appender.metrics())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET).build();
        table.newAppend().appendFile(committed).commit();
        return table;
    }

    /**
     * The flag, proven end to end on the shipped routing: {@code variantProjectionPushdownWithDeletes} decides which
     * read a delete-bearing task takes, so ON must yield the pruned read at a fraction of the bytes, OFF must yield the
     * standard full-width read — the same byte count as Iceberg's own delete path — and both must return identical
     * rows. On a task with <em>no</em> deletes the flag must be inert, because it gates only the delete composition.
     * <p>
     * This is the break-glass switch, so it has to be shown to actually switch. The correctness mirrors in the cluster
     * suites prove flag-off gives the same answers; only the byte count can prove it gives them the slow way.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Drives openPrunedReadIfEligible with the flag on and off over one delete-bearing task and "
            + "asserts by byte count that off restores the full standard read while on prunes")
    @Test
    public void theFlagTurnsDeleteAwarePruningOnAndOff() throws Exception {
        File warehouse = warehouse();
        Table table = createWideTable(warehouse);
        String path = singleTask(table).file().location();

        // Before any delete exists, the flag must make no difference: both settings take the pruned read.
        FileScanTask noDeletes = singleTask(table);
        Assert.assertTrue(noDeletes.deletes().isEmpty());
        for (boolean flag : new boolean[] { true, false }) {
            try (CloseableIterable<Record> read = IcebergFileRecordReader.openPrunedReadIfEligible(table.io(),
                    table.io().newInputFile(path), noDeletes, table.schema(), SCHEMA, narrowingPlan(), flag)) {
                Assert.assertNotNull("a delete-free task is pruned regardless of the deletes flag (flag=" + flag + ")",
                        read);
            }
        }

        long[] dv = new long[WIDE_ROWS / 50];
        for (int i = 0; i < dv.length; i++) {
            dv[i] = i * 50L;
        }
        table.newRowDelta().addDeletes(writeDeletionVector(table, path, "dv", dv)).commit();
        FileScanTask task = singleTask(table);
        Assert.assertFalse(task.deletes().isEmpty());

        // Flag ON: the shipped routing must hand back the pruned delete-aware read.
        TrackingInputFile onFile = new TrackingInputFile(table.io().newInputFile(path));
        long onStart = System.nanoTime();
        List<Integer> onRows = new ArrayList<>();
        try (CloseableIterable<Record> read = IcebergFileRecordReader.openPrunedReadIfEligible(table.io(), onFile, task,
                table.schema(), SCHEMA, narrowingPlan(), true)) {
            Assert.assertNotNull("flag on: the delete-bearing task must take the pruned read", read);
            for (Record r : read) {
                onRows.add((Integer) r.getField("id"));
            }
        }
        long onMillis = (System.nanoTime() - onStart) / 1_000_000;

        // Flag OFF: the shipped routing must decline, and the caller then runs the standard delete path — exactly the
        // read IcebergFileRecordReader.setNextRecordsIterator falls through to. Measure that read.
        TrackingInputFile offFile = new TrackingInputFile(table.io().newInputFile(path));
        Assert.assertNull("flag off: the delete-bearing task must be declined and take the standard read",
                IcebergFileRecordReader.openPrunedReadIfEligible(table.io(), offFile, task, table.schema(), SCHEMA,
                        narrowingPlan(), false));
        long offStart = System.nanoTime();
        List<Integer> offRows = icebergOracle(table, task, offFile, SCHEMA);
        long offMillis = (System.nanoTime() - offStart) / 1_000_000;

        // And the plain standard read, as a yardstick for what "off" must cost.
        TrackingInputFile standardFile = new TrackingInputFile(table.io().newInputFile(path));
        List<Integer> standardRows = icebergOracle(table, task, standardFile, SCHEMA);

        Assert.assertEquals("flag on and flag off must return identical rows", offRows, onRows);
        Assert.assertEquals(standardRows, onRows);
        Assert.assertEquals(WIDE_ROWS - dv.length, onRows.size());

        long onBytes = onFile.bytesRead.get();
        long offBytes = offFile.bytesRead.get();
        long standardBytes = standardFile.bytesRead.get();
        System.out.printf(
                "variantProjectionPushdownWithDeletes on a deletion-vector task (%d rows, %d fields, %d deleted): "
                        + "ON %d bytes in %d ms, OFF %d bytes in %d ms, standard delete path %d bytes "
                        + "(on = %.1f%% of off)%n",
                WIDE_ROWS, WIDE_FIELDS, dv.length, onBytes, onMillis, offBytes, offMillis, standardBytes,
                100.0 * onBytes / offBytes);
        Assert.assertEquals("flag OFF must be the standard read, byte for byte — the switch restores the old path, "
                + "not some third one", standardBytes, offBytes);
        Assert.assertTrue("flag ON must read well under half the bytes of flag OFF; on=" + onBytes + " off=" + offBytes,
                onBytes * 2 < offBytes);
    }
}
