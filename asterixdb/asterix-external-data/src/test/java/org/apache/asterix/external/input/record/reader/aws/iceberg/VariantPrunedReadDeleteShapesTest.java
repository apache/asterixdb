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
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
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
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * The pruned delete-aware read across the <em>table shapes</em> production actually has and the earlier suites did
 * not reach: partitioned tables, equality keys of every common type, a key column added by schema evolution, a
 * variant nested in a struct, two variant columns, and an unreadable equality-delete file.
 * <p>
 * The standard is correctness, full stop. Every positive case here compares the surviving {@code id}s from the shipped
 * pruned composition against Iceberg's own standard delete-aware read of the <b>same</b> task — the read this feature
 * replaces — and demands row-for-row equality. Losing pruning on some shape would be acceptable; returning a row the
 * standard path removes, or dropping one it keeps, is the one outcome none of these tests may pass on.
 */
public class VariantPrunedReadDeleteShapesTest {

    private static final int ROWS_PER_FILE = 120;
    private static final String ROW_GROUP_BYTES = "8192";

    // ------------------------------------------------------------------------------------------------ fixtures

    private static File warehouse() throws Exception {
        return java.nio.file.Files.createTempDirectory("variant-delete-shapes").resolve("tbl").toFile();
    }

    /**
     * NOTE: {@code HadoopTables.create} reassigns every field id, so the {@link Schema} objects built in the tests are
     * only a description of the table — projections must always be derived from {@code table.schema()}, or nested
     * field ids will not match the data files and Iceberg's reader will report the field as missing.
     */
    private static Table newTable(File warehouse, Schema schema, PartitionSpec spec) {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, ROW_GROUP_BYTES);
        return new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema, spec, props,
                warehouse.toString());
    }

    /** {@code { <keep>: <id>, big: <varying padding> }} — {@code big} is what pruning must leave unread. */
    private static Variant variant(VariantMetadata meta, String keep, int id) {
        ShreddedObject object = Variants.object(meta);
        object.put(keep, Variants.of(id));
        object.put("big", Variants.of(("padding-" + id).repeat(12)));
        return Variant.of(meta, object);
    }

    private static Type shreddedType(VariantValue sample) throws Exception {
        java.lang.reflect.Method m = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        m.setAccessible(true);
        return (Type) m.invoke(null, sample);
    }

    /** Writes one data file of {@code rows} with the table's CURRENT schema, committing it under {@code partition}. */
    private static DataFile writeDataFile(Table table, String name, List<Record> rows, StructLike partition,
            BiFunction<Integer, String, Type> shredding) throws Exception {
        File file = new File(new File(table.location(), "data"), name + ".parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        FileAppender<Record> appender = Parquet.write(org.apache.iceberg.Files.localOutput(file)).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc(shredding::apply)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, ROW_GROUP_BYTES).build();
        try (FileAppender<Record> w = appender) {
            for (Record r : rows) {
                w.add(r);
            }
        }
        DataFiles.Builder builder =
                DataFiles.builder(table.spec()).withInputFile(org.apache.iceberg.Files.localInput(file))
                        .withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET);
        if (partition != null) {
            builder.withPartition(partition);
        }
        DataFile dataFile = builder.build();
        table.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    /** An equality delete on the named columns, one delete row per entry of {@code keys}, optionally partitioned. */
    private static DeleteFile writeEqualityDelete(Table table, String name, List<String> keyColumns,
            List<Map<String, Object>> keys, StructLike partition) throws Exception {
        Schema deleteSchema = table.schema().select(keyColumns);
        List<Integer> ids = new ArrayList<>();
        for (String c : keyColumns) {
            ids.add(table.schema().findField(c).fieldId());
        }
        File file = new File(new File(table.location(), "deletes"), name + ".parquet");
        Assert.assertTrue(file.getParentFile().mkdirs() || file.getParentFile().exists());
        Parquet.DeleteWriteBuilder builder =
                Parquet.writeDeletes(org.apache.iceberg.Files.localOutput(file)).forTable(table).rowSchema(deleteSchema)
                        .createWriterFunc(GenericParquetWriter::create).equalityFieldIds(ids).overwrite();
        if (partition != null) {
            builder.withPartition(partition);
        }
        EqualityDeleteWriter<Record> writer = builder.buildEqualityWriter();
        GenericRecord template = GenericRecord.create(deleteSchema);
        try (EqualityDeleteWriter<Record> w = writer) {
            for (Map<String, Object> key : keys) {
                Record row = template.copy();
                key.forEach(row::setField);
                w.write(row);
            }
        }
        return writer.toDeleteFile();
    }

    private static List<FileScanTask> tasks(Table table) throws Exception {
        List<FileScanTask> out = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(out::add);
        }
        return out;
    }

    private static VariantProjectionPlan plan(Schema projection, List<List<String>> paths) throws Exception {
        ARecordType projected = ProjectionFiltrationTypeUtil.getRecordType(paths);
        VariantProjectionPlan plan = VariantProjectionPlan.from(projection, projected, true);
        Assert.assertFalse("the plan must narrow at least one variant", plan.isEmpty());
        return plan;
    }

    // ------------------------------------------------------------------------------------------------- oracle

    /** Iceberg's standard delete-aware read of one task — the read this feature replaces. */
    private static List<Integer> oracle(Table table, FileScanTask task, Schema projection) throws Exception {
        org.apache.iceberg.data.GenericDeleteFilter filter =
                new org.apache.iceberg.data.GenericDeleteFilter(table.io(), task, table.schema(), projection);
        Schema required = filter.requiredSchema();
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows = Parquet.read(table.io().newInputFile(task.file().location()))
                .project(required).filter(task.residual()).split(task.start(), task.length())
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(required, fs)).build()) {
            for (Record r : filter.filter(rows)) {
                ids.add((Integer) r.getField("id"));
            }
        }
        return ids;
    }

    /** The shipped pruned composition of one task, asserting pruning actually happened and running {@code check}. */
    private static List<Integer> pruned(Table table, FileScanTask task, Schema projection, VariantProjectionPlan plan,
            Consumer<Record> check) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> live = IcebergFileRecordReader.openPrunedDeleteAwareRead(table.io(),
                table.io().newInputFile(task.file().location()), task, table.schema(), projection, plan)) {
            Assert.assertNotNull("the shipped code must have taken the pruned path for " + task.file().location(),
                    live);
            for (Record r : live) {
                ids.add((Integer) r.getField("id"));
                check.accept(r);
            }
        }
        return ids;
    }

    /**
     * Every planned task, oracle versus pruned, row for row. Returns the union of surviving ids so a test can make its
     * own shape-specific assertions on top of the agreement.
     */
    private static List<Integer> assertEveryTaskMatchesOracle(Table table, Schema projection,
            VariantProjectionPlan plan, Consumer<Record> check) throws Exception {
        List<FileScanTask> planned = tasks(table);
        Assert.assertFalse(planned.isEmpty());
        List<Integer> union = new ArrayList<>();
        for (FileScanTask task : planned) {
            List<Integer> expected = oracle(table, task, projection);
            List<Integer> actual = pruned(table, task, projection, plan, check);
            Assert.assertEquals("pruned read must match Iceberg's standard delete-aware read row for row on "
                    + task.file().location() + " (deletes=" + task.deletes().size() + ")", expected, actual);
            union.addAll(actual);
        }
        Collections.sort(union);
        return union;
    }

    private static Consumer<Record> bigMustBeUnread(String column) {
        return r -> Assert.assertNull("the unreferenced sub-field must not have been read",
                ((Variant) r.getField(column)).value().asObject().get("big"));
    }

    // ------------------------------------------------------------------------------------------------- shapes

    /**
     * A table partitioned by {@code bucket}, one data file per partition, with an equality delete scoped to ONE
     * partition. Equality deletes attach per partition, so only that partition's task may carry it, and the rows it
     * names must vanish there and nowhere else.
     */
    @Test
    public void partitionedTable_equalityDeleteScopedToOnePartition() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "v", Types.VariantType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("bucket").build();
        Table table = newTable(warehouse(), schema, spec);
        VariantMetadata meta = Variants.metadata("x", "big");
        Type typed = shreddedType(variant(meta, "x", 0).value());

        int partitions = 4;
        for (int b = 0; b < partitions; b++) {
            List<Record> rows = new ArrayList<>();
            GenericRecord template = GenericRecord.create(table.schema());
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                int id = b * 1000 + i;
                Record r = template.copy();
                r.setField("id", id);
                r.setField("bucket", b);
                r.setField("name", "n" + (i % 6));
                r.setField("v", variant(meta, "x", id));
                rows.add(r);
            }
            PartitionKey key = new PartitionKey(spec, table.schema());
            key.partition(rows.get(0));
            writeDataFile(table, "p" + b, rows, key, (fid, name) -> typed);
        }

        // Delete name == "n2" in partition bucket=2 only.
        GenericRecord probe = GenericRecord.create(table.schema());
        probe.setField("bucket", 2);
        PartitionKey p2 = new PartitionKey(spec, table.schema());
        p2.partition(probe);
        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-p2", List.of("name"), List.of(Map.of("name", "n2")), p2))
                .commit();

        List<FileScanTask> planned = tasks(table);
        Assert.assertEquals(partitions, planned.size());
        int tasksWithDeletes = 0;
        for (FileScanTask t : planned) {
            if (!t.deletes().isEmpty()) {
                tasksWithDeletes++;
                Assert.assertEquals("the delete must be scoped to partition 2", 2,
                        t.file().partition().get(0, Integer.class).intValue());
            }
        }
        Assert.assertEquals("exactly one partition carries the equality delete", 1, tasksWithDeletes);

        Schema projection = table.schema().select("id", "v");
        List<Integer> survivors = assertEveryTaskMatchesOracle(table, projection,
                plan(projection, List.of(List.of("v", "x"))), bigMustBeUnread("v"));

        Assert.assertEquals(partitions * ROWS_PER_FILE - ROWS_PER_FILE / 6, survivors.size());
        for (int id : survivors) {
            boolean inPartition2 = id / 1000 == 2;
            boolean isN2 = (id % 1000) % 6 == 2;
            Assert.assertFalse("n2 rows of partition 2 must be gone: " + id, inPartition2 && isN2);
        }
        Assert.assertTrue("n2 rows of OTHER partitions must survive",
                survivors.contains(2 + 0 * 1000) && survivors.contains(1000 + 2) && survivors.contains(3000 + 2));
    }

    /**
     * Equality keys of every common non-integer type — string, date, timestamp, decimal — one delete file each. The
     * keep-predicate matches through {@code InternalRecordWrapper}, which converts values by type and position; a
     * mismatch there would compare the wrong representation and keep every row.
     */
    @Test
    public void equalityKeysOfStringDateTimestampAndDecimal() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "skey", Types.StringType.get()),
                Types.NestedField.required(3, "dkey", Types.DateType.get()),
                Types.NestedField.required(4, "tskey", Types.TimestampType.withoutZone()),
                Types.NestedField.required(5, "deckey", Types.DecimalType.of(10, 2)),
                Types.NestedField.optional(6, "v", Types.VariantType.get()));
        Table table = newTable(warehouse(), schema, PartitionSpec.unpartitioned());
        VariantMetadata meta = Variants.metadata("x", "big");
        Type typed = shreddedType(variant(meta, "x", 0).value());
        LocalDate d0 = LocalDate.of(2026, 1, 1);
        LocalDateTime t0 = LocalDateTime.of(2026, 1, 1, 0, 0);

        List<Record> rows = new ArrayList<>();
        GenericRecord template = GenericRecord.create(table.schema());
        for (int id = 0; id < ROWS_PER_FILE * 3; id++) {
            Record r = template.copy();
            r.setField("id", id);
            r.setField("skey", "s" + (id % 7));
            r.setField("dkey", d0.plusDays(id % 5));
            r.setField("tskey", t0.plusHours(id % 3));
            r.setField("deckey", new BigDecimal(id % 4).setScale(2));
            r.setField("v", variant(meta, "x", id));
            rows.add(r);
        }
        writeDataFile(table, "rows", rows, null, (fid, name) -> typed);

        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-s", List.of("skey"), List.of(Map.of("skey", "s3")), null))
                .addDeletes(writeEqualityDelete(table, "eq-d", List.of("dkey"), List.of(Map.of("dkey", d0.plusDays(4))),
                        null))
                .addDeletes(writeEqualityDelete(table, "eq-ts", List.of("tskey"),
                        List.of(Map.of("tskey", t0.plusHours(1))), null))
                .addDeletes(writeEqualityDelete(table, "eq-dec", List.of("deckey"),
                        List.of(Map.of("deckey", new BigDecimal("2.00"))), null))
                .commit();
        Assert.assertEquals(4, tasks(table).get(0).deletes().size());

        Schema projection = table.schema().select("id", "v");
        List<Integer> survivors = assertEveryTaskMatchesOracle(table, projection,
                plan(projection, List.of(List.of("v", "x"))), bigMustBeUnread("v"));

        Assert.assertFalse(survivors.isEmpty());
        for (int id : survivors) {
            Assert.assertNotEquals("skey s3 must be gone: " + id, 3, id % 7);
            Assert.assertNotEquals("dkey +4 days must be gone: " + id, 4, id % 5);
            Assert.assertNotEquals("tskey +1h must be gone: " + id, 1, id % 3);
            Assert.assertNotEquals("deckey 2.00 must be gone: " + id, 2, id % 4);
        }
        // and each delete removed something, so no key type was silently ignored
        int expectedGone = 0;
        for (int id = 0; id < ROWS_PER_FILE * 3; id++) {
            if (id % 7 == 3 || id % 5 == 4 || id % 3 == 1 || id % 4 == 2) {
                expectedGone++;
            }
        }
        Assert.assertEquals(ROWS_PER_FILE * 3 - expectedGone, survivors.size());
    }

    /**
     * Schema evolution: the equality key column is added AFTER the first data file was written. That file has no such
     * column at all — its rows read the key as null and must all survive — while the later file's rows match and are
     * removed. Both files carry the delete, since the table is unpartitioned.
     */
    @Test
    public void equalityKeyColumnAddedBySchemaEvolution_oldFileKeepsEveryRow() throws Exception {
        Schema initial = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "v", Types.VariantType.get()));
        Table table = newTable(warehouse(), initial, PartitionSpec.unpartitioned());
        VariantMetadata meta = Variants.metadata("x", "big");
        Type typed = shreddedType(variant(meta, "x", 0).value());

        List<Record> old = new ArrayList<>();
        GenericRecord oldTemplate = GenericRecord.create(table.schema());
        for (int id = 0; id < ROWS_PER_FILE; id++) {
            Record r = oldTemplate.copy();
            r.setField("id", id);
            r.setField("v", variant(meta, "x", id));
            old.add(r);
        }
        writeDataFile(table, "before-evolution", old, null, (fid, name) -> typed);

        table.updateSchema().addColumn("tag", Types.StringType.get()).commit();
        Assert.assertNotNull(table.schema().findField("tag"));

        List<Record> fresh = new ArrayList<>();
        GenericRecord newTemplate = GenericRecord.create(table.schema());
        for (int id = 1000; id < 1000 + ROWS_PER_FILE; id++) {
            Record r = newTemplate.copy();
            r.setField("id", id);
            r.setField("tag", id % 2 == 0 ? "even" : "odd");
            r.setField("v", variant(meta, "x", id));
            fresh.add(r);
        }
        writeDataFile(table, "after-evolution", fresh, null, (fid, name) -> typed);

        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-tag", List.of("tag"), List.of(Map.of("tag", "odd")), null))
                .commit();
        for (FileScanTask t : tasks(table)) {
            Assert.assertEquals("unpartitioned: every file carries the delete", 1, t.deletes().size());
        }

        Schema projection = table.schema().select("id", "v");
        List<Integer> survivors = assertEveryTaskMatchesOracle(table, projection,
                plan(projection, List.of(List.of("v", "x"))), bigMustBeUnread("v"));

        for (int id = 0; id < ROWS_PER_FILE; id++) {
            Assert.assertTrue("pre-evolution row must survive (its key reads as null): " + id, survivors.contains(id));
        }
        for (int id = 1000; id < 1000 + ROWS_PER_FILE; id++) {
            Assert.assertEquals("post-evolution odd rows must be gone: " + id, id % 2 == 0, survivors.contains(id));
        }
    }

    /** A variant nested in a struct, with a deletion vector and an equality delete on the same task. */
    @Test
    public void variantNestedInStruct_withBothDeleteKinds() throws Exception {
        Types.StructType st = Types.StructType.of(Types.NestedField.required(10, "tag", Types.StringType.get()),
                Types.NestedField.optional(11, "v", Types.VariantType.get()));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
                Types.NestedField.optional(3, "st", st));
        Table table = newTable(warehouse(), schema, PartitionSpec.unpartitioned());
        VariantMetadata meta = Variants.metadata("x", "big");
        Type typed = shreddedType(variant(meta, "x", 0).value());

        List<Record> rows = new ArrayList<>();
        GenericRecord template = GenericRecord.create(table.schema());
        GenericRecord stTemplate = GenericRecord.create(st);
        for (int id = 0; id < ROWS_PER_FILE * 2; id++) {
            Record inner = stTemplate.copy();
            inner.setField("tag", "t" + id);
            inner.setField("v", variant(meta, "x", id));
            Record r = template.copy();
            r.setField("id", id);
            r.setField("bucket", id % 5);
            r.setField("st", inner);
            rows.add(r);
        }
        DataFile data = writeDataFile(table, "nested", rows, null, (fid, name) -> typed);

        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-b1", List.of("bucket"), List.of(Map.of("bucket", 1)), null))
                .addDeletes(deletionVector(table, data.location(), 0, 5, 10, 239)).commit();
        Assert.assertEquals(2, tasks(table).get(0).deletes().size());

        Schema projection = table.schema().select("id", "st");
        Consumer<Record> check = r -> {
            Record inner = (Record) r.getField("st");
            Assert.assertNotNull("the struct must reconstruct", inner);
            Assert.assertNull("the unreferenced nested sub-field must not have been read",
                    ((Variant) inner.getField("v")).value().asObject().get("big"));
        };
        List<Integer> survivors = assertEveryTaskMatchesOracle(table, projection,
                plan(projection, List.of(List.of("st", "v", "x"))), check);

        for (int gone : new int[] { 0, 5, 10, 239 }) {
            Assert.assertFalse("vector position " + gone + " must be gone", survivors.contains(gone));
        }
        for (int id : survivors) {
            Assert.assertNotEquals("bucket 1 must be gone: " + id, 1, id % 5);
        }
    }

    /** Two variant columns, both narrowed, with an equality delete whose key is in neither. */
    @Test
    public void twoVariantColumns_bothPrunedUnderAnEqualityDelete() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
                Types.NestedField.optional(3, "v1", Types.VariantType.get()),
                Types.NestedField.optional(4, "v2", Types.VariantType.get()));
        Table table = newTable(warehouse(), schema, PartitionSpec.unpartitioned());
        VariantMetadata meta1 = Variants.metadata("x", "big");
        VariantMetadata meta2 = Variants.metadata("y", "big");
        Type typed1 = shreddedType(variant(meta1, "x", 0).value());
        Type typed2 = shreddedType(variant(meta2, "y", 0).value());

        List<Record> rows = new ArrayList<>();
        GenericRecord template = GenericRecord.create(table.schema());
        for (int id = 0; id < ROWS_PER_FILE * 2; id++) {
            Record r = template.copy();
            r.setField("id", id);
            r.setField("bucket", id % 8);
            r.setField("v1", variant(meta1, "x", id));
            r.setField("v2", variant(meta2, "y", id));
            rows.add(r);
        }
        writeDataFile(table, "two", rows, null, (fid, name) -> "v1".equals(name) ? typed1 : typed2);
        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-b6", List.of("bucket"), List.of(Map.of("bucket", 6)), null))
                .commit();

        Schema projection = table.schema().select("id", "v1", "v2");
        Consumer<Record> check = r -> {
            Assert.assertNull(((Variant) r.getField("v1")).value().asObject().get("big"));
            Assert.assertNull(((Variant) r.getField("v2")).value().asObject().get("big"));
            Assert.assertNotNull(((Variant) r.getField("v1")).value().asObject().get("x"));
            Assert.assertNotNull(((Variant) r.getField("v2")).value().asObject().get("y"));
        };
        List<Integer> survivors = assertEveryTaskMatchesOracle(table, projection,
                plan(projection, List.of(List.of("v1", "x"), List.of("v2", "y"))), check);
        Assert.assertEquals(ROWS_PER_FILE * 2 - ROWS_PER_FILE * 2 / 8, survivors.size());
        for (int id : survivors) {
            Assert.assertNotEquals(6, id % 8);
        }
    }

    /**
     * The equality-delete file cannot be read. The equality set loads lazily — at the moment the keep-predicate is
     * built, which is after the reader is open — so this is a different failure point from the deletion-vector case
     * and needs its own proof that nothing is emitted and the reader is closed.
     */
    @Test
    public void unreadableEqualityDeleteFile_failsBeforeAnyRowAndClosesTheReader() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "bucket", Types.IntegerType.get()),
                Types.NestedField.optional(3, "v", Types.VariantType.get()));
        Table table = newTable(warehouse(), schema, PartitionSpec.unpartitioned());
        VariantMetadata meta = Variants.metadata("x", "big");
        Type typed = shreddedType(variant(meta, "x", 0).value());
        List<Record> rows = new ArrayList<>();
        GenericRecord template = GenericRecord.create(table.schema());
        for (int id = 0; id < ROWS_PER_FILE; id++) {
            Record r = template.copy();
            r.setField("id", id);
            r.setField("bucket", id % 3);
            r.setField("v", variant(meta, "x", id));
            rows.add(r);
        }
        DataFile data = writeDataFile(table, "rows", rows, null, (fid, name) -> typed);
        table.newRowDelta()
                .addDeletes(writeEqualityDelete(table, "eq-b0", List.of("bucket"), List.of(Map.of("bucket", 0)), null))
                .commit();
        FileScanTask task = tasks(table).get(0);

        Schema projection = table.schema().select("id", "v");
        CountingInputFile tracked = new CountingInputFile(table.io().newInputFile(data.location()));
        FileIO faulty = new FailOnSuffixFileIO(table.io(), "/deletes/");
        try {
            IcebergFileRecordReader.openPrunedDeleteAwareRead(faulty, tracked, task, table.schema(), projection,
                    plan(projection, List.of(List.of("v", "x"))));
            Assert.fail("loading the equality delete must fail loudly");
        } catch (RuntimeException | java.io.IOException expected) {
            // the injected failure
        }
        Assert.assertTrue("the data file was opened", tracked.opened > 0);
        Assert.assertEquals("and closed again on the way out", 0, tracked.open);
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private static DeleteFile deletionVector(Table table, String dataFilePath, long... positions) throws Exception {
        String path = table.location() + "/deletes/dv-" + positions.length + ".puffin";
        org.apache.iceberg.deletes.DVFileWriter writer =
                new org.apache.iceberg.deletes.BaseDVFileWriter(() -> table.io().newOutputFile(path), ignored -> null);
        try (org.apache.iceberg.deletes.DVFileWriter w = writer) {
            for (long p : positions) {
                w.delete(dataFilePath, p, table.spec(), null);
            }
        }
        List<DeleteFile> written = writer.result().deleteFiles();
        Assert.assertEquals(1, written.size());
        return written.get(0);
    }

    private static final class FailOnSuffixFileIO implements FileIO {
        private final FileIO delegate;
        private final String fragment;

        FailOnSuffixFileIO(FileIO delegate, String fragment) {
            this.delegate = delegate;
            this.fragment = fragment;
        }

        @Override
        public InputFile newInputFile(String location) {
            if (location.contains(fragment)) {
                throw new org.apache.iceberg.exceptions.RuntimeIOException(new java.io.IOException("injected"),
                        "injected failure reading %s", location);
            }
            return delegate.newInputFile(location);
        }

        @Override
        public OutputFile newOutputFile(String location) {
            return delegate.newOutputFile(location);
        }

        @Override
        public void deleteFile(String location) {
            delegate.deleteFile(location);
        }
    }

    private static final class CountingInputFile implements InputFile {
        private final InputFile delegate;
        int opened;
        int open;

        CountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public org.apache.iceberg.io.SeekableInputStream newStream() {
            org.apache.iceberg.io.SeekableInputStream inner = delegate.newStream();
            opened++;
            open++;
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
                    return inner.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws java.io.IOException {
                    return inner.read(b, off, len);
                }

                @Override
                public void close() throws java.io.IOException {
                    inner.close();
                    open--;
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

}
