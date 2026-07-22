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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.iceberg.TableScan;
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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Data-loss-focused tests for {@link VariantBoundsEvaluator}, run against <b>real Iceberg tables</b> (local
 * {@link HadoopTables}) so bounds are written to and read back from actual Avro manifests — the round trip that broke
 * Iceberg's own evaluator and that in-memory {@code Metrics} fixtures do not exercise.
 * <p>
 * The central invariant is exhaustive: for every predicate and every data file, <b>if the file contains a row that
 * really matches, the evaluator must keep it</b>. The expected answer is computed from the rows themselves rather than
 * hand-written, so the test cannot silently agree with a buggy evaluator.
 *
 * <p>TODO(iceberg-15384): this class tests {@link VariantBoundsEvaluator}, which exists only because Iceberg 1.11.0
 * cannot read variant bounds out of a manifest. When <a href="https://github.com/apache/iceberg/pull/15384">
 * apache/iceberg#15384</a> is merged <b>and</b> included in the Iceberg release this build depends on, and that
 * evaluator is deleted, re-point this coverage at Iceberg's own
 * {@code InclusiveMetricsEvaluator} rather than deleting it — the type matrix and the never-lose-a-row invariant stay
 * just as valuable against their implementation.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Exhaustive no-data-loss tests for variant sub-field bound pruning over real manifests: verifies "
        + "every kept/dropped decision against the actual rows, plus conservative fallbacks")
public class VariantBoundsEvaluatorTest {

    private static final String COLUMN = "variant_field";
    private static Table intTable;
    private static Table stringTable;
    private static Table mixedTable;
    /** bucket value per data file for intTable (one row per file). */
    private static final int[] BUCKETS = { 0, 1, 2, 5, 9, 10, 42, 100 };
    private static final String[] NAMES = { "alpha", "bravo", "delta", "echo" };

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, COLUMN, Types.VariantType.get()));
    }

    @BeforeClass
    public static void setup() throws Exception {
        intTable = createTable("int");
        for (int i = 0; i < BUCKETS.length; i++) {
            appendShreddedFile(intTable, "b_" + i, "bucket", Variants.of(BUCKETS[i]), i + 1);
        }
        stringTable = createTable("string");
        for (int i = 0; i < NAMES.length; i++) {
            appendShreddedFile(stringTable, "s_" + i, "name", Variants.of(NAMES[i]), i + 1);
        }
        // A file whose sub-field holds a STRING while the query will compare against an int, and vice versa.
        mixedTable = createTable("mixed");
        appendShreddedFile(mixedTable, "m_int", "val", Variants.of(7), 1);
        appendShreddedFile(mixedTable, "m_str", "val", Variants.of("seven"), 2);
    }

    /**
     * A bound the evaluator cannot parse must keep the file AND say so.
     * <p>
     * This is the one failure path in this class that can be provoked: the bounds are supplied directly as bytes, so
     * they can be made deliberately malformed, where the {@code mightMatch} catch can only be reached by an
     * unforeseen bug. Keeping the file is the safe answer; the warning is what stops "bounds stopped being readable"
     * from looking indistinguishable from "this predicate does not prune much".
     * <p>
     * The warning names the sub-field, never the data file: it is the actionable part, and being identical for every
     * file in the scan it folds into one warning instead of thousands.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Provokes the unparseable-bound path with hand-written garbage bytes and asserts the file is kept and one warning naming the sub-field is raised")
    @Test
    public void unparseableBound_keepsTheFileAndWarnsOnce() throws Exception {
        Table table = createTable("garbage-bounds");
        java.nio.ByteBuffer garbage = java.nio.ByteBuffer.wrap(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
        Map<Integer, java.nio.ByteBuffer> bounds = new HashMap<>();
        bounds.put(2, garbage); // field id 2 is the variant column
        Metrics metrics = new Metrics(1L, null, null, null, null, bounds, bounds);
        String path = table.location() + "/data/garbage.parquet";
        // No data file is written: planning only reads the manifest, and the bounds are what is under test.
        AppendFiles append = table.newAppend();
        append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1L).withMetrics(metrics).build());
        append.commit();

        org.apache.asterix.common.exceptions.WarningCollector warnings =
                new org.apache.asterix.common.exceptions.WarningCollector();
        List<String> kept = keptFiles(table, rewrite(COLUMN + ".bucket", 5, "eq"), warnings);
        Assert.assertEquals("an unparseable bound must never skip the file", List.of("garbage.parquet"), kept);

        List<org.apache.hyracks.api.exceptions.Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertEquals("lower and upper both fail, but the message is identical so it folds into one", 1,
                raised.size());
        Assert.assertEquals(
                org.apache.asterix.common.exceptions.ErrorCode.ICEBERG_VARIANT_BOUNDS_NOT_EVALUATED.intValue(),
                raised.get(0).getCode());
        Assert.assertTrue("the warning must name the sub-field, got: " + raised.get(0).getMessage(),
                raised.get(0).getMessage().contains("bucket"));
        Assert.assertFalse("the warning must not carry a file path: " + raised.get(0).getMessage(),
                raised.get(0).getMessage().contains("garbage.parquet"));
    }

    private static Table createTable(String name) throws Exception {
        File dir = java.nio.file.Files.createTempDirectory("vbe-" + name).toFile();
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        return new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema(),
                PartitionSpec.unpartitioned(), props, new File(dir, "tbl").getAbsolutePath());
    }

    /** Writes one fully-shredded single-row data file and commits it with real metrics. */
    private static void appendShreddedFile(Table table, String fileName, String field, VariantValue value, int id)
            throws Exception {
        VariantMetadata meta = Variants.metadata(field);
        ShreddedObject obj = Variants.object(meta);
        obj.put(field, value);
        Variant variant = Variant.of(meta, obj);
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.parquet.schema.Type typed =
                (org.apache.parquet.schema.Type) toParquetSchema.invoke(null, variant.value());

        String path = table.location() + "/data/" + fileName + ".parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord template = GenericRecord.create(table.schema());
        Record row = template.copy();
        row.setField("id", id);
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

    /**
     * @return the file names the evaluator decides to keep, planned with stats exactly as the reader factory does.
     *         Sorted, because {@code planFiles()} makes no ordering guarantee.
     */
    private static List<String> keptFiles(Table table, Expression rewritten) throws Exception {
        return keptFiles(table, rewritten, new org.apache.asterix.common.exceptions.WarningCollector());
    }

    private static List<String> keptFiles(Table table, Expression rewritten,
            org.apache.hyracks.api.exceptions.IWarningCollector warnings) throws Exception {
        VariantBoundsEvaluator evaluator = new VariantBoundsEvaluator(table.schema(), rewritten, warnings);
        List<String> kept = new ArrayList<>();
        TableScan scan = table.newScan().includeColumnStats();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                if (evaluator.mightMatch(task.file())) {
                    kept.add(new File(task.file().location()).getName());
                }
            }
        }
        java.util.Collections.sort(kept);
        return kept;
    }

    /**
     * Writes ONE data file whose {@code typed_value} for {@code field} is int32, but which also contains a row whose
     * value at that path is a {@code double} — a type that cannot live in an int32 {@code typed_value} and so must be
     * written to the variant's residual {@code value} blob instead.
     *
     * @param typedValues values that match the shredded int32 type and so become typed_value rows
     * @param residual    a value of a different physical type, forced into the residual blob; {@code null}
     *                    writes an all-shredded control file instead
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Builds a file mixing shredded and residual values at the SAME variant path, to test whether Iceberg's manifest bounds account for residual values")
    private static void appendMixedShreddingFile(Table table, String fileName, String field, int[] typedValues,
            VariantValue residual) throws Exception {
        VariantMetadata meta = Variants.metadata(field);
        // The shredding schema is derived from an int32 template, and reused for every row in the file.
        ShreddedObject template = Variants.object(meta);
        template.put(field, Variants.of(typedValues[0]));
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.parquet.schema.Type typed =
                (org.apache.parquet.schema.Type) toParquetSchema.invoke(null, Variant.of(meta, template).value());

        String path = table.location() + "/data/" + fileName + ".parquet";
        OutputFile out = table.io().newOutputFile(path);
        GenericRecord rowTemplate = GenericRecord.create(table.schema());
        int id = 1;
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fid, n) -> typed).build()) {
            for (int typedValue : typedValues) {
                ShreddedObject obj = Variants.object(meta);
                obj.put(field, Variants.of(typedValue));
                Record row = rowTemplate.copy();
                row.setField("id", id++);
                row.setField(COLUMN, Variant.of(meta, obj));
                writer.add(row);
            }
            if (residual != null) {
                ShreddedObject obj = Variants.object(meta);
                obj.put(field, residual);
                Record row = rowTemplate.copy();
                row.setField("id", id);
                row.setField(COLUMN, Variant.of(meta, obj));
                writer.add(row);
            }
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(path), MetricsConfig.forTable(table));
        AppendFiles append = table.newAppend();
        append.appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build());
        append.commit();
    }

    /**
     * The soundness question this whole optimization rests on: do a variant sub-field's manifest bounds account for
     * values at that path which were written to the residual blob instead of the shredded {@code typed_value} column?
     * <p>
     * The file here holds {@code x} = 10 and 20 as shredded int32 values, plus {@code x} = 5.0 as a double, which
     * cannot go in an int32 {@code typed_value} and is therefore written to the residual. If the bounds cover only the
     * typed column they are {@code [10, 20]}, and a scan for {@code x = 5.0} would prove "no row can match" and skip
     * the file — losing a row that really does match. If the bounds account for the residual too, the file is kept.
     * <p>
     * Nothing else in either harness exercises a single variant path that is shredded in some rows and residual in
     * others, which is why this is asserted directly rather than inferred.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Asserts that a file is not skipped for a value living in the variant residual blob rather than the shredded typed_value column; the assumption all file skipping depends on")
    @Test
    public void residualValueAtAShreddedPath_doesNotGetTheFileSkipped() throws Exception {
        Table table = createTable("residual");
        appendMixedShreddingFile(table, "mixed_shredding", "x", new int[] { 10, 20 }, Variants.of(5.0d));

        Expression forResidual = Expressions.equal(Expressions.extract(COLUMN, "$.x", "double"), 5.0d);
        List<String> kept = keptFiles(table, forResidual);
        Assert.assertEquals("a row with x = 5.0 lives in this file's residual blob, so the file must not be skipped; "
                + "if it was skipped, a variant sub-field's manifest bounds do not cover residual values and file "
                + "skipping can drop rows: " + boundsReport(table), List.of("mixed_shredding.parquet"), kept);
    }

    /**
     * Explains <em>why</em> {@link #residualValueAtAShreddedPath_doesNotGetTheFileSkipped} is safe, and is the real
     * guard of the two.
     * <p>
     * Iceberg does not publish partial bounds. A file whose variant path is fully shredded gets bounds for that path;
     * add a single row whose value at the same path cannot go in the shredded column — so it lands in the residual blob
     * — and Iceberg emits <b>no bounds at all</b> for the variant column. That is what makes file skipping sound: the
     * evaluator is never handed bounds that describe only part of a file's rows, and with no bounds it keeps the file.
     * <p>
     * Without this test, the sibling test above passes vacuously: a kept file proves nothing when the reason it was
     * kept is that no bounds existed. It also turns the soundness property into a tripwire — if a future Iceberg ever
     * starts emitting typed-column-only bounds for a partially shredded path, this fails, and
     * {@link VariantBoundsEvaluator} would then be able to skip a file that holds a matching residual row.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Tripwire: Iceberg publishes bounds for a fully shredded variant path but none once any value at that path is residual; this all-or-nothing behaviour is what makes data-file skipping sound")
    @Test
    public void icebergPublishesNoBoundsWhenAPathIsPartlyResidual() throws Exception {
        Table allShredded = createTable("all-shredded");
        appendMixedShreddingFile(allShredded, "all_typed", "x", new int[] { 10, 20 }, null);
        Assert.assertNotEquals("a fully shredded variant path must have bounds, otherwise this test proves nothing "
                + "about the partly-residual case below", "<none>", soleLowerBound(allShredded));

        Table partlyResidual = createTable("partly-residual");
        appendMixedShreddingFile(partlyResidual, "partly", "x", new int[] { 10, 20 }, Variants.of(5.0d));
        Assert.assertEquals(
                "one residual value at a shredded path must suppress the variant column's bounds "
                        + "entirely; typed-column-only bounds would let a matching residual row be skipped",
                "<none>", soleLowerBound(partlyResidual));
    }

    /** @return the rendered lower bound of the variant column of the table's single data file. */
    private static String soleLowerBound(Table table) throws Exception {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            List<String> rendered = new ArrayList<>();
            for (FileScanTask task : tasks) {
                rendered.add(describeBound(task.file().lowerBounds()));
            }
            Assert.assertEquals("expected exactly one data file", 1, rendered.size());
            return rendered.get(0);
        }
    }

    /** @return a human-readable dump of the variant column's lower/upper bounds, for failure messages. */
    private static String boundsReport(Table table) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                sb.append(new File(task.file().location()).getName()).append(" lower=")
                        .append(describeBound(task.file().lowerBounds())).append(" upper=")
                        .append(describeBound(task.file().upperBounds()));
            }
        }
        return sb.toString();
    }

    private static String describeBound(Map<Integer, java.nio.ByteBuffer> bounds) {
        java.nio.ByteBuffer buffer = bounds == null ? null : bounds.get(2);
        if (buffer == null) {
            return "<none>";
        }
        try {
            Variant variant = Variant.from(buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN));
            return variant.value().asObject().toString();
        } catch (RuntimeException e) {
            return "<unparseable: " + e + ">";
        }
    }

    private static Expression rewrite(String dottedRef, Object literal, String op) {
        Expression e;
        switch (op) {
            case "eq":
                e = Expressions.equal(dottedRef, literal);
                break;
            case "lt":
                e = Expressions.lessThan(dottedRef, literal);
                break;
            case "lteq":
                e = Expressions.lessThanOrEqual(dottedRef, literal);
                break;
            case "gt":
                e = Expressions.greaterThan(dottedRef, literal);
                break;
            case "gteq":
                e = Expressions.greaterThanOrEqual(dottedRef, literal);
                break;
            case "noteq":
                e = Expressions.notEqual(dottedRef, literal);
                break;
            default:
                throw new IllegalArgumentException(op);
        }
        return VariantPredicateRewriter.rewriteAssumingNesting(e, schema());
    }

    /** True when a single-row file holding {@code actual} really matches {@code op literal}. */
    private static boolean rowMatches(long actual, String op, long literal) {
        switch (op) {
            case "eq":
                return actual == literal;
            case "lt":
                return actual < literal;
            case "lteq":
                return actual <= literal;
            case "gt":
                return actual > literal;
            case "gteq":
                return actual >= literal;
            case "noteq":
                return actual != literal;
            default:
                throw new IllegalArgumentException(op);
        }
    }

    /**
     * THE critical test: across every operator and a wide range of literals, the set of kept files must include every
     * file that genuinely matches. Any violation is silent data loss.
     */
    @Test
    public void neverDropsAFileThatContainsAMatchingRow() throws Exception {
        List<String> ops = Arrays.asList("eq", "lt", "lteq", "gt", "gteq", "noteq");
        long[] literals = { -5, 0, 1, 2, 3, 5, 9, 10, 11, 42, 99, 100, 101, 1000 };
        int checks = 0;
        for (String op : ops) {
            for (long lit : literals) {
                List<String> kept = keptFiles(intTable, rewrite(COLUMN + ".bucket", lit, op));
                for (int i = 0; i < BUCKETS.length; i++) {
                    String file = "b_" + i + ".parquet";
                    if (rowMatches(BUCKETS[i], op, lit)) {
                        Assert.assertTrue(
                                "DATA LOSS: dropped " + file + " (bucket=" + BUCKETS[i] + ") for " + op + " " + lit,
                                kept.contains(file));
                        checks++;
                    }
                }
            }
        }
        Assert.assertTrue("expected many matching-row checks, got " + checks, checks > 100);
    }

    /** The flip side: pruning must actually happen, else the whole exercise is pointless. */
    @Test
    public void prunesFilesThatCannotMatch() throws Exception {
        // one row per bucket value, so equality keeps exactly one file
        Assert.assertEquals(List.of("b_3.parquet"), keptFiles(intTable, rewrite(COLUMN + ".bucket", 5L, "eq")));
        // nothing matches
        Assert.assertEquals(List.of(), keptFiles(intTable, rewrite(COLUMN + ".bucket", 999999L, "eq")));
        // buckets < 2 -> {0, 1}
        Assert.assertEquals(List.of("b_0.parquet", "b_1.parquet"),
                keptFiles(intTable, rewrite(COLUMN + ".bucket", 2L, "lt")));
        // buckets >= 42 -> {42, 100}
        Assert.assertEquals(List.of("b_6.parquet", "b_7.parquet"),
                keptFiles(intTable, rewrite(COLUMN + ".bucket", 42L, "gteq")));
        // NOT_EQ is never used to prune (nulls make it unsafe): all files kept
        Assert.assertEquals(BUCKETS.length, keptFiles(intTable, rewrite(COLUMN + ".bucket", 5L, "noteq")).size());
    }

    /** String bounds must prune correctly too, and never drop a genuine match. */
    @Test
    public void stringBoundsPruneAndNeverLoseRows() throws Exception {
        Assert.assertEquals(List.of("s_2.parquet"), keptFiles(stringTable, rewrite(COLUMN + ".name", "delta", "eq")));
        Assert.assertEquals(List.of(), keptFiles(stringTable, rewrite(COLUMN + ".name", "zulu", "eq")));
        for (int i = 0; i < NAMES.length; i++) {
            Assert.assertTrue("DATA LOSS for " + NAMES[i],
                    keptFiles(stringTable, rewrite(COLUMN + ".name", NAMES[i], "eq")).contains("s_" + i + ".parquet"));
        }
    }

    /**
     * Mixed types across files: comparing an int literal must not drop the file whose value is a string (its bound is
     * a string, which we cannot compare — so we must keep it), and vice versa.
     */
    @Test
    public void mixedTypeSubFieldIsNeverPrunedUnsafely() throws Exception {
        List<String> keptForInt = keptFiles(mixedTable, rewrite(COLUMN + ".val", 7L, "eq"));
        Assert.assertTrue("int-valued file must be kept", keptForInt.contains("m_int.parquet"));
        Assert.assertTrue("string-valued file is not comparable to an int literal -> must be kept",
                keptForInt.contains("m_str.parquet"));

        List<String> keptForString = keptFiles(mixedTable, rewrite(COLUMN + ".val", "seven", "eq"));
        Assert.assertTrue("string-valued file must be kept", keptForString.contains("m_str.parquet"));
        Assert.assertTrue("int-valued file is not comparable to a string literal -> must be kept",
                keptForString.contains("m_int.parquet"));
    }

    /** A sub-field that is not in the bounds at all (never shredded) must never be pruned. */
    @Test
    public void unknownSubFieldIsNeverPruned() throws Exception {
        Assert.assertEquals(BUCKETS.length, keptFiles(intTable, rewrite(COLUMN + ".doesNotExist", 5L, "eq")).size());
    }

    /** AND may prune when either side proves impossibility; OR only when both do. */
    @Test
    public void booleanCombinationsAreSound() throws Exception {
        Schema s = schema();
        Expression bothImpossible = VariantPredicateRewriter.rewriteAssumingNesting(Expressions.and(
                Expressions.equal(COLUMN + ".bucket", 999999L), Expressions.equal(COLUMN + ".bucket", 888888L)), s);
        Assert.assertEquals(List.of(), keptFiles(intTable, bothImpossible));

        Expression oneImpossible = VariantPredicateRewriter.rewriteAssumingNesting(Expressions
                .or(Expressions.equal(COLUMN + ".bucket", 5L), Expressions.equal(COLUMN + ".bucket", 999999L)), s);
        Assert.assertEquals("OR keeps the file matching the possible side", List.of("b_3.parquet"),
                keptFiles(intTable, oneImpossible));

        Expression negated = VariantPredicateRewriter
                .rewriteAssumingNesting(Expressions.not(Expressions.equal(COLUMN + ".bucket", 5L)), s);
        Assert.assertEquals("NOT is not modelled -> keep everything", BUCKETS.length,
                keptFiles(intTable, negated).size());
    }

    /** A field name containing a dot must not be rewritten at all (dot-notation cannot express it). */
    @Test
    public void dottedFieldNameIsNotRewritten() {
        Expression original = Expressions.equal(COLUMN + ".a.b", 5L);
        Expression rewritten = VariantPredicateRewriter.rewriteAssumingNesting(original, schema());
        // "a.b" is ambiguous, but a genuinely nested a -> b is legitimate, so this must rewrite as nested:
        Assert.assertTrue(VariantPredicateRewriter.containsExtractTerm(rewritten));
        Assert.assertTrue(rewritten.toString().contains("$.a.b"));
    }

    /** Bound-key conversion: the manifest keys use bracket notation even though extract paths use dots. */
    @Test
    public void boundKeyConversion() {
        Assert.assertEquals("$['a']", VariantPredicateRewriter.toBoundKey("$.a"));
        Assert.assertEquals("$['a']['b']", VariantPredicateRewriter.toBoundKey("$.a.b"));
        Assert.assertNull(VariantPredicateRewriter.toBoundKey("a"));
    }
}
