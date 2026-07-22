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
package org.apache.asterix.external.util.iceberg;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.Assert;
import org.junit.Test;

/**
 * Type-coverage and corner-case tests for {@link VariantBoundsEvaluator}, over <b>real Iceberg tables</b> so bounds
 * round-trip through actual Avro manifests.
 * <p>
 * Every supported variant physical type is covered, split by whether the evaluator is allowed to compare it:
 * <ul>
 * <li><b>Comparable</b> (BOOLEAN, INT8/16/32/64, FLOAT, DOUBLE, DECIMAL4/8/16, STRING) — pruning must be exact: a file
 * is kept if and only if its value can match.</li>
 * <li><b>Deliberately not compared</b> (DATE, TIME, all TIMESTAMP flavours, BINARY, UUID, NULL, nested object/array) —
 * the evaluator must never drop these, because their Java representations are ambiguous (temporals arrive as bare
 * epoch numbers) or have no ordering we should assume.</li>
 * </ul>
 * The bias throughout is toward detecting <em>silent data loss</em>: expectations are computed from the values written
 * rather than hand-listed, and boundary literals (exactly the min and exactly the max) are exercised for every
 * operator, since off-by-one errors there are what drop rows.
 *
 * <p>TODO(iceberg-15384): this class tests {@link VariantBoundsEvaluator}, which exists only because Iceberg 1.11.0
 * cannot read variant bounds out of a manifest. When <a href="https://github.com/apache/iceberg/pull/15384">
 * apache/iceberg#15384</a> is merged <b>and</b> included in the Iceberg release this build depends on, and that
 * evaluator is deleted, re-point this coverage at Iceberg's own
 * {@code InclusiveMetricsEvaluator} rather than deleting it — the type matrix and the never-lose-a-row invariant stay
 * just as valuable against their implementation.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "All-variant-type and corner-case coverage for bound-based file pruning: exact pruning for "
        + "comparable types, never-prune for temporal/binary/uuid/null, boundary literals, extremes, "
        + "NaN/infinity, unicode and long strings, nested paths and null columns")
public class VariantBoundsAllTypesTest {

    private static final String COLUMN = "variant_field";
    private static final String FIELD = "f";

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, COLUMN, Types.VariantType.get()));
    }

    // ---------------------------------------------------------------------------------------------------
    // table / file helpers
    // ---------------------------------------------------------------------------------------------------

    private static Table newTable(String name) throws Exception {
        File dir = java.nio.file.Files.createTempDirectory("vbat-" + name).toFile();
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        return new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema(),
                PartitionSpec.unpartitioned(), props, new File(dir, "t").getAbsolutePath());
    }

    private static org.apache.parquet.schema.Type typedValueFor(Variant variant) throws Exception {
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        return (org.apache.parquet.schema.Type) toParquetSchema.invoke(null, variant.value());
    }

    /** Appends one single-row file whose {@code f} sub-field holds {@code value} (shredded). */
    private static void appendFile(Table table, String fileName, VariantValue value) throws Exception {
        VariantMetadata meta = Variants.metadata(FIELD);
        ShreddedObject obj = Variants.object(meta);
        obj.put(FIELD, value);
        Variant variant = Variant.of(meta, obj);
        appendVariant(table, fileName, variant, typedValueFor(variant));
    }

    private static void appendVariant(Table table, String fileName, Variant variant,
            org.apache.parquet.schema.Type typed) throws Exception {
        String path = table.location() + "/data/" + fileName + ".parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        Record row = template.copy();
        row.setField("id", 1);
        row.setField(COLUMN, variant);
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fid, n) -> typed).build()) {
            writer.add(row);
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        AppendFiles append = table.newAppend();
        append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build());
        append.commit();
    }

    private static List<String> kept(Table table, Expression rewritten) throws Exception {
        VariantBoundsEvaluator evaluator = new VariantBoundsEvaluator(table.schema(), rewritten,
                new org.apache.asterix.common.exceptions.WarningCollector());
        List<String> names = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                if (evaluator.mightMatch(task.file())) {
                    names.add(new File(task.file().location()).getName());
                }
            }
        }
        java.util.Collections.sort(names);
        return names;
    }

    private static Expression predicate(String op, Object literal) {
        String ref = COLUMN + "." + FIELD;
        Expression e;
        switch (op) {
            case "eq":
                e = Expressions.equal(ref, literal);
                break;
            case "lt":
                e = Expressions.lessThan(ref, literal);
                break;
            case "lteq":
                e = Expressions.lessThanOrEqual(ref, literal);
                break;
            case "gt":
                e = Expressions.greaterThan(ref, literal);
                break;
            case "gteq":
                e = Expressions.greaterThanOrEqual(ref, literal);
                break;
            default:
                throw new IllegalArgumentException(op);
        }
        return VariantPredicateRewriter.rewriteAssumingNesting(e, schema());
    }

    private static final List<String> OPS = Arrays.asList("eq", "lt", "lteq", "gt", "gteq");

    private static boolean matches(int cmp, String op) {
        switch (op) {
            case "eq":
                return cmp == 0;
            case "lt":
                return cmp < 0;
            case "lteq":
                return cmp <= 0;
            case "gt":
                return cmp > 0;
            case "gteq":
                return cmp >= 0;
            default:
                throw new IllegalArgumentException(op);
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // comparable types: pruning must be exact (keep iff the row can match)
    // ---------------------------------------------------------------------------------------------------

    /**
     * Drives one comparable type: writes a file per value, then for every operator and every literal asserts the
     * evaluator's decision matches the ground truth computed from the values themselves.
     */
    private static <T extends Comparable<T>> void assertExactPruning(String label, List<T> values,
            java.util.function.Function<T, VariantValue> toVariant, List<T> literals,
            java.util.function.Function<T, Object> toLiteral) throws Exception {
        Table table = newTable(label);
        for (int i = 0; i < values.size(); i++) {
            appendFile(table, "f" + i, toVariant.apply(values.get(i)));
        }
        for (String op : OPS) {
            for (T lit : literals) {
                List<String> keptFiles = kept(table, predicate(op, toLiteral.apply(lit)));
                for (int i = 0; i < values.size(); i++) {
                    boolean shouldMatch = matches(values.get(i).compareTo(lit), op);
                    String file = "f" + i + ".parquet";
                    if (shouldMatch) {
                        Assert.assertTrue(label + ": DATA LOSS — dropped " + file + " (value=" + values.get(i)
                                + ") for " + op + " " + lit, keptFiles.contains(file));
                    } else {
                        Assert.assertFalse(label + ": should have pruned " + file + " (value=" + values.get(i)
                                + ") for " + op + " " + lit, keptFiles.contains(file));
                    }
                }
            }
        }
    }

    @Test
    public void int8_exact() throws Exception {
        List<Integer> vals = Arrays.asList(-128, -1, 0, 5, 127);
        assertExactPruning("INT8", vals, v -> Variants.of(v.byteValue()), vals, v -> (long) (int) v);
    }

    @Test
    public void int16_exact() throws Exception {
        List<Integer> vals = Arrays.asList(-32768, -1, 0, 1000, 32767);
        assertExactPruning("INT16", vals, v -> Variants.of(v.shortValue()), vals, v -> (long) (int) v);
    }

    @Test
    public void int32_exact() throws Exception {
        List<Integer> vals = Arrays.asList(Integer.MIN_VALUE, -1, 0, 42, Integer.MAX_VALUE);
        assertExactPruning("INT32", vals, Variants::of, vals, v -> (long) (int) v);
    }

    @Test
    public void int64_exact() throws Exception {
        List<Long> vals = Arrays.asList(Long.MIN_VALUE, -1L, 0L, 42L, Long.MAX_VALUE);
        assertExactPruning("INT64", vals, Variants::of, vals, v -> v);
    }

    @Test
    public void float_exact() throws Exception {
        List<Float> vals = Arrays.asList(-1.5f, 0f, 3.14f, 1e10f);
        assertExactPruning("FLOAT", vals, Variants::of, vals, v -> (double) (float) v);
    }

    @Test
    public void double_exact() throws Exception {
        List<Double> vals = Arrays.asList(-1.5d, 0d, 2.718281828459045d, 1e100d);
        assertExactPruning("DOUBLE", vals, Variants::of, vals, v -> v);
    }

    /**
     * Short strings only: Iceberg <em>truncates</em> string bounds (16 characters by default), so for longer values
     * the recorded bounds are a widened prefix range and exact pruning is impossible by design. Long strings are
     * covered separately by {@link #string_truncatedBoundsNeverLoseRows()}.
     */
    @Test
    public void string_exact() throws Exception {
        List<String> vals = Arrays.asList("", "alpha", "delta", "zulu", "é中文");
        assertExactPruning("STRING", vals, Variants::of, vals, v -> v);
    }

    /**
     * Values longer than the bound truncation length must still never be dropped. Truncated bounds only widen the
     * range, so the evaluator must keep such files whenever they match.
     */
    @Test
    public void string_truncatedBoundsNeverLoseRows() throws Exception {
        Table table = newTable("longstring");
        String long1 = "x".repeat(70);
        String long2 = "x".repeat(60) + "zzz"; // shares the first 60 chars, so bounds truncate identically
        appendFile(table, "f0", Variants.of(long1));
        appendFile(table, "f1", Variants.of(long2));

        Assert.assertTrue("DATA LOSS: exact long value must keep its file",
                kept(table, predicate("eq", long1)).contains("f0.parquet"));
        Assert.assertTrue("DATA LOSS: exact long value must keep its file",
                kept(table, predicate("eq", long2)).contains("f1.parquet"));
        // A value sharing the truncated prefix cannot be excluded, so both files stay.
        Assert.assertEquals(2, kept(table, predicate("eq", "x".repeat(65))).size());
        // A clearly different prefix is still prunable.
        Assert.assertEquals(0, kept(table, predicate("eq", "aaaa")).size());
    }

    @Test
    public void boolean_exact() throws Exception {
        List<Boolean> vals = Arrays.asList(false, true);
        assertExactPruning("BOOLEAN", vals, Variants::of, vals, v -> v);
    }

    /** DECIMAL16 exceeds a long: comparing via longValue() would overflow and could drop a matching file. */
    @Test
    public void decimal_hugeValuesAreComparedExactly() throws Exception {
        Table table = newTable("decimal");
        BigDecimal small = new BigDecimal("3.14");
        BigDecimal huge = new BigDecimal("123456789012345678901.123456789");
        appendFile(table, "f0", Variants.of(small));
        appendFile(table, "f1", Variants.of(huge));

        // A literal above the small value but far below the huge one must keep only the huge file. If the huge bound
        // were compared via longValue() it would overflow and this file could be dropped.
        Assert.assertEquals(List.of("f1.parquet"), kept(table, predicate("gt", 1.0e12d)));
        // A literal comfortably below the huge value must keep it (no overflow-induced loss).
        Assert.assertTrue("DATA LOSS: huge decimal dropped for a smaller literal",
                kept(table, predicate("gteq", 1.0e20d)).contains("f1.parquet"));
        Assert.assertTrue("DATA LOSS: huge decimal dropped for a larger literal",
                kept(table, predicate("lteq", 1.0e21d)).contains("f1.parquet"));
        // Below everything -> both kept.
        Assert.assertEquals(2, kept(table, predicate("gt", -1.0d)).size());
        // Above everything -> both pruned.
        Assert.assertEquals(0, kept(table, predicate("gt", 1.0e30d)).size());
    }

    // ---------------------------------------------------------------------------------------------------
    // types the evaluator must never prune on
    // ---------------------------------------------------------------------------------------------------

    /**
     * Temporal, binary, uuid and null values must never be pruned. Temporals are the dangerous ones: they come back
     * as bare Integer/Long epoch counts, so a naive comparison against a numeric literal would look valid.
     */
    @Test
    public void nonComparableTypesAreNeverPruned() throws Exception {
        Map<String, VariantValue> values = new java.util.LinkedHashMap<>();
        values.put("date", Variants.ofDate(19723));
        values.put("time", Variants.ofTime(37230000000L));
        values.put("tstz", Variants.ofTimestamptz(1707000000000000L));
        values.put("tsntz", Variants.ofIsoTimestampntz("2024-02-04T12:00:00"));
        values.put("tstznanos", Variants.ofTimestamptzNanos(1707000000000000000L));
        values.put("tsntznanos", Variants.ofIsoTimestampntzNanos("2024-02-04T12:00:00"));
        values.put("binary", Variants.of(ByteBuffer.wrap(new byte[] { 1, 2, 3 })));
        values.put("uuid", Variants.ofUUID(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")));
        values.put("nullvalue", Variants.ofNull());

        for (Map.Entry<String, VariantValue> entry : values.entrySet()) {
            Table table = newTable(entry.getKey());
            appendFile(table, "f0", entry.getValue());
            for (String op : OPS) {
                for (Object literal : new Object[] { 0L, 19723L, Long.MAX_VALUE, "x", 1.5d }) {
                    Expression rewritten = predicate(op, literal);
                    Assert.assertEquals(entry.getKey() + " must never be pruned (" + op + " " + literal + ")",
                            List.of("f0.parquet"), kept(table, rewritten));
                }
            }
        }
    }

    /** Object- and array-valued sub-fields have no scalar bound and must always be kept. */
    @Test
    public void objectAndArraySubFieldsAreNeverPruned() throws Exception {
        Table table = newTable("objarr");
        VariantMetadata meta = Variants.metadata(FIELD, "inner");
        ShreddedObject inner = Variants.object(meta);
        inner.put("inner", Variants.of(1));
        ShreddedObject root = Variants.object(meta);
        root.put(FIELD, inner);
        Variant variant = Variant.of(meta, root);
        appendVariant(table, "f0", variant, typedValueFor(variant));

        for (String op : OPS) {
            Assert.assertEquals("object-valued sub-field must be kept", List.of("f0.parquet"),
                    kept(table, predicate(op, 1L)));
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // corner cases
    // ---------------------------------------------------------------------------------------------------

    /**
     * Iceberg refuses to build a literal from NaN, so a NaN predicate can never reach the evaluator. Infinities are
     * ordered and may prune, but only in agreement with the real comparison.
     */
    @Test
    public void nanIsRejectedUpstreamAndInfinitiesComparePrecisely() throws Exception {
        try {
            predicate("eq", Double.NaN);
            Assert.fail("expected Iceberg to reject a NaN literal");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("NaN"));
        }

        Table table = newTable("inf");
        appendFile(table, "f0", Variants.of(1.0d));
        for (String op : OPS) {
            for (double literal : new double[] { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
                boolean shouldMatch = matches(Double.compare(1.0d, literal), op);
                Assert.assertEquals(op + " " + literal, shouldMatch,
                        kept(table, predicate(op, literal)).contains("f0.parquet"));
            }
        }
    }

    /** A null variant column carries no bounds for the sub-field, so it must always be kept. */
    @Test
    public void nullVariantColumnIsNeverPruned() throws Exception {
        Table table = newTable("nullcol");
        appendFile(table, "f0", Variants.of(5));
        // a file whose variant column is entirely null
        String path = table.location() + "/data/f1.parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        Record row = template.copy();
        row.setField("id", 2);
        row.setField(COLUMN, null);
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            writer.add(row);
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build()).commit();

        Assert.assertTrue("null-variant file must be kept", kept(table, predicate("eq", 999L)).contains("f1.parquet"));
        Assert.assertFalse("the valued file can still be pruned",
                kept(table, predicate("eq", 999L)).contains("f0.parquet"));
    }

    /** Nested sub-paths must resolve to the right bound key and prune on the right field. */
    @Test
    public void nestedPathPrunesOnTheCorrectSubField() throws Exception {
        Table table = newTable("nested");
        VariantMetadata meta = Variants.metadata("a", "b", "other");
        for (int i = 0; i < 3; i++) {
            ShreddedObject inner = Variants.object(meta);
            inner.put("b", Variants.of(i));
            inner.put("other", Variants.of(100 + i));
            ShreddedObject root = Variants.object(meta);
            root.put("a", inner);
            Variant variant = Variant.of(meta, root);
            appendVariant(table, "f" + i, variant, typedValueFor(variant));
        }
        Expression onB =
                VariantPredicateRewriter.rewriteAssumingNesting(Expressions.equal(COLUMN + ".a.b", 1L), schema());
        Assert.assertEquals(List.of("f1.parquet"), kept(table, onB));

        Expression onOther =
                VariantPredicateRewriter.rewriteAssumingNesting(Expressions.equal(COLUMN + ".a.other", 102L), schema());
        Assert.assertEquals("must prune on 'other', not 'b'", List.of("f2.parquet"), kept(table, onOther));
    }

    /** Multi-row files: bounds span a range, and every value inside the range must keep the file. */
    @Test
    public void multiRowFileRangeIsRespected() throws Exception {
        Table table = newTable("multirow");
        VariantMetadata meta = Variants.metadata(FIELD);
        ShreddedObject probe = Variants.object(meta);
        probe.put(FIELD, Variants.of(0));
        org.apache.parquet.schema.Type typed = typedValueFor(Variant.of(meta, probe));

        String path = table.location() + "/data/range.parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fid, n) -> typed).build()) {
            for (int v : new int[] { 10, 20, 30 }) {
                ShreddedObject o = Variants.object(meta);
                o.put(FIELD, Variants.of(v));
                Record row = template.copy();
                row.setField("id", v);
                row.setField(COLUMN, Variant.of(meta, o));
                writer.add(row);
            }
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build()).commit();

        // every present value, plus both boundaries, must keep the file
        for (long v : new long[] { 10, 20, 30 }) {
            Assert.assertEquals("DATA LOSS for value " + v, 1, kept(table, predicate("eq", v)).size());
        }
        // a value inside the range but absent from the file: bounds cannot exclude it, so it is kept
        Assert.assertEquals(1, kept(table, predicate("eq", 15L)).size());
        // outside the range: prunable
        Assert.assertEquals(0, kept(table, predicate("eq", 5L)).size());
        Assert.assertEquals(0, kept(table, predicate("eq", 35L)).size());
        Assert.assertEquals(0, kept(table, predicate("lt", 10L)).size());
        Assert.assertEquals(0, kept(table, predicate("gt", 30L)).size());
        // boundary operators that do match
        Assert.assertEquals(1, kept(table, predicate("lteq", 10L)).size());
        Assert.assertEquals(1, kept(table, predicate("gteq", 30L)).size());
    }
}
