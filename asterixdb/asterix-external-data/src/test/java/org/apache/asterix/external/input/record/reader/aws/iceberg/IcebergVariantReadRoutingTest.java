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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * Which read path a data file takes, per task.
 * <p>
 * Deletes are a per-task property, so within a single scan some files can be read with variant sub-columns pruned
 * while others fall back to the delete-aware read. Nothing downstream reveals which branch a file took — rows and
 * {@code processedObjects} are identical either way — so the routing decision is asserted directly here.
 * <p>
 * The distinction matters because the two delete kinds behave differently: a <b>deletion vector</b> is file-scoped, so
 * a table with DVs on a few files still gets pruned reads for the rest; an <b>equality delete</b> attaches to every
 * file of a partition and so disables pruning for the whole scan.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Asserts per-task routing between the variant-pruned read and the delete-aware read, including the mixed scan a deletion-vector table produces")
public class IcebergVariantReadRoutingTest {

    private static final String COLUMN = "variant_field";
    /** Stands in for a DV attached to one data file; only its presence is consulted. */
    private static final List<DeleteFile> ONE_DELETE = Collections.singletonList(null);
    private static final List<DeleteFile> NO_DELETES = Collections.emptyList();

    @Test
    public void narrowedPlanAndNoDeletes_isPruned() throws Exception {
        Assert.assertTrue(IcebergFileRecordReader.shouldTryPrunedVariantRead(narrowingPlan(), NO_DELETES));
    }

    @Test
    public void narrowedPlanButDeletesPresent_fallsBack() throws Exception {
        Assert.assertFalse(IcebergFileRecordReader.shouldTryPrunedVariantRead(narrowingPlan(), ONE_DELETE));
    }

    @Test
    public void emptyPlan_isNeverPruned() throws Exception {
        Assert.assertFalse(
                IcebergFileRecordReader.shouldTryPrunedVariantRead(VariantProjectionPlan.none(), NO_DELETES));
        Assert.assertFalse(
                IcebergFileRecordReader.shouldTryPrunedVariantRead(VariantProjectionPlan.none(), ONE_DELETE));
    }

    /** A task may report null rather than an empty list; that is not "has deletes". */
    @Test
    public void nullDeletes_isTreatedAsNone() throws Exception {
        Assert.assertTrue(IcebergFileRecordReader.shouldTryPrunedVariantRead(narrowingPlan(), null));
    }

    /**
     * The deletion-vector table's shape: DVs on two of its files, none on the rest. Exactly the DV-free files must be
     * pruned, in the same scan — the interleaving that an equality-delete table can never produce.
     */
    @Test
    public void deletionVectorTable_prunesOnlyTheFilesWithoutVectors() throws Exception {
        VariantProjectionPlan plan = narrowingPlan();
        int fileCount = 21;
        List<Integer> filesWithVectors = Arrays.asList(3, 20);

        List<Integer> pruned = new ArrayList<>();
        List<Integer> fellBack = new ArrayList<>();
        for (int file = 0; file < fileCount; file++) {
            List<DeleteFile> deletes = filesWithVectors.contains(file) ? ONE_DELETE : NO_DELETES;
            (IcebergFileRecordReader.shouldTryPrunedVariantRead(plan, deletes) ? pruned : fellBack).add(file);
        }

        Assert.assertEquals("only the DV-bearing files fall back", filesWithVectors, fellBack);
        Assert.assertEquals("every other file is pruned", fileCount - filesWithVectors.size(), pruned.size());
        Assert.assertFalse("the mix must actually be a mix", pruned.isEmpty() || fellBack.isEmpty());
    }

    /** An equality delete attaches to every file of the partition, so nothing in the scan is pruned. */
    @Test
    public void equalityDeleteTable_prunesNothing() throws Exception {
        VariantProjectionPlan plan = narrowingPlan();
        for (int file = 0; file < 20; file++) {
            Assert.assertFalse("every task carries the equality delete",
                    IcebergFileRecordReader.shouldTryPrunedVariantRead(plan, ONE_DELETE));
        }
    }

    private static long sum(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).sum();
    }

    /**
     * An unbindable filter must be dropped <em>and</em> reported. Silently discarding it would turn a mistyped column
     * name into unexplained slowness with nothing to go on; the warning names the filter that was skipped.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Asserts the unbindable-filter fail-safe both drops the filter and raises ICEBERG_FILTER_NOT_PUSHED, since the drop is otherwise invisible")
    @Test
    public void unbindableFilter_isDroppedAndWarned() {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(org.apache.iceberg.types.Types.NestedField
                .required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
        WarningCollector warnings = new WarningCollector();
        org.apache.iceberg.expressions.Expression unbindable =
                org.apache.iceberg.expressions.Expressions.equal("nosuchcolumn", 1);

        org.apache.iceberg.expressions.Expression pushed =
                IcebergParquetRecordReaderFactory.bindableOrNoFilter(unbindable, schema, warnings);

        Assert.assertEquals("an unbindable filter must not be pushed",
                org.apache.iceberg.expressions.Expressions.alwaysTrue(), pushed);
        List<Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertEquals("exactly one warning", 1, raised.size());
        Assert.assertEquals(ErrorCode.ICEBERG_FILTER_NOT_PUSHED.intValue(), raised.get(0).getCode());
        Assert.assertTrue("the warning must name the filter that was skipped, got: " + raised.get(0).getMessage(),
                raised.get(0).getMessage().contains("nosuchcolumn"));
    }

    /** A filter that binds is pushed unchanged and must not produce a warning. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Guards against the fail-safe warning on every ordinary query, which would make the signal useless")
    /**
     * One unbindable reference must not cost the pushdown of everything ANDed with it. {@code Binder.bind} is
     * all-or-nothing over a whole expression, so the filter has to be weakened part by part instead of discarded.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins that only the unbindable conjunct is dropped, so a typo in one predicate no longer disables pushdown for the rest")
    @Test
    public void mixedBindableAndUnbindable_keepsThePushablePart() {
        Schema schema = filterSchema();
        WarningCollector warnings = new WarningCollector();
        org.apache.iceberg.expressions.Expression good = org.apache.iceberg.expressions.Expressions.equal("id", 1);
        org.apache.iceberg.expressions.Expression mixed = org.apache.iceberg.expressions.Expressions.and(good,
                org.apache.iceberg.expressions.Expressions.equal("nosuchcolumn", 2));

        org.apache.iceberg.expressions.Expression pushed =
                IcebergParquetRecordReaderFactory.bindableOrNoFilter(mixed, schema, warnings);
        List<Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertEquals("the bindable conjunct must survive", good.toString(), pushed.toString());
        Assert.assertEquals("exactly one warning, for the dropped conjunct", 1, raised.size());
        Assert.assertTrue("the warning must name the part that was dropped, got: " + raised.get(0).getMessage(),
                raised.get(0).getMessage().contains("nosuchcolumn"));
    }

    /** An OR cannot be weakened piecewise — dropping a disjunct strengthens the filter — so it goes whole. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Guards the weaken-only rule: an OR holding an unbindable side is dropped whole rather than half-kept")
    @Test
    public void unbindableInsideOr_dropsTheWholeDisjunction() {
        Schema schema = filterSchema();
        WarningCollector warnings = new WarningCollector();
        org.apache.iceberg.expressions.Expression mixed = org.apache.iceberg.expressions.Expressions
                .and(org.apache.iceberg.expressions.Expressions.equal("id", 1),
                        org.apache.iceberg.expressions.Expressions.or(
                                org.apache.iceberg.expressions.Expressions.equal("id", 2),
                                org.apache.iceberg.expressions.Expressions.equal("nosuchcolumn", 3)));

        org.apache.iceberg.expressions.Expression pushed =
                IcebergParquetRecordReaderFactory.bindableOrNoFilter(mixed, schema, warnings);
        List<Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertEquals("only the standalone bindable conjunct may remain",
                org.apache.iceberg.expressions.Expressions.equal("id", 1).toString(), pushed.toString());
        Assert.assertEquals(1, raised.size());
    }

    @Test
    public void bindableFilter_isPushedWithoutWarning() {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(org.apache.iceberg.types.Types.NestedField
                .required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
        WarningCollector warnings = new WarningCollector();
        org.apache.iceberg.expressions.Expression bindable = org.apache.iceberg.expressions.Expressions.equal("id", 1);

        Assert.assertSame(bindable, IcebergParquetRecordReaderFactory.bindableOrNoFilter(bindable, schema, warnings));
        List<Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertTrue("no warning for a filter that pushes fine", raised.isEmpty());
    }

    /**
     * Executes the projection-fallback warning path, which nothing reachable can trigger.
     * <p>
     * Both callers are defensive catches around a read that is not supposed to fail, and every guard on that path
     * fails cleanly rather than throwing — the clipper returns the schema unchanged when it cannot narrow, the group
     * rebuild returns null instead of raising, and "nothing to prune" is an ordinary no-warning fallback. So the
     * {@code warn} call itself would never run under test, and a mistake in it (an unresolvable code, a message that
     * varies per file and so defeats deduplication) would only surface in production.
     * <p>
     * Two different causes are reported deliberately, and only ONE warning comes out: the collector folds identical
     * warnings together rather than repeating them. That is the whole reason the message carries no file name and no
     * exception text — a message that varied per file would defeat the folding and emit one warning per data file,
     * on a path that runs once per file across every partition.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Runs the otherwise-unexecuted projection-fallback warning path, pinning that the code resolves to a message and that the message does not vary with the cause")
    @Test
    public void projectionFallback_raisesOneConstantWarning() throws Exception {
        WarningCollector warnings = new WarningCollector();
        IcebergFileRecordReader reader =
                new IcebergFileRecordReader(List.of(), filterSchema(), new HashMap<>(), warnings);

        reader.warnProjectionNotPushed(new RuntimeException("first file blew up"));
        reader.warnProjectionNotPushed(new IllegalStateException("a completely different failure"));

        List<Warning> raised = new ArrayList<>();
        warnings.getWarnings(raised, Long.MAX_VALUE);
        Assert.assertEquals("two fallbacks with different causes must collapse into one warning, not one per file", 1,
                raised.size());
        Warning warning = raised.get(0);
        Assert.assertEquals(ErrorCode.ICEBERG_VARIANT_PROJECTION_NOT_PUSHED.intValue(), warning.getCode());
        Assert.assertNotNull("the code must resolve to a message", warning.getMessage());
        Assert.assertFalse("the message must be filled in, not a raw format string: " + warning.getMessage(),
                warning.getMessage().isEmpty() || warning.getMessage().contains("%1$s"));
        Assert.assertFalse(
                "no exception text may leak into the message, or repeats stop collapsing: " + warning.getMessage(),
                warning.getMessage().contains("blew up"));
    }

    /** A trivial schema with one bindable column, for the filter-weakening tests. */
    private static Schema filterSchema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    }

    private static VariantProjectionPlan narrowingPlan() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, COLUMN, Types.VariantType.get()));
        ARecordType projected =
                ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of(COLUMN, "bucket"), List.of("id")));
        VariantProjectionPlan plan = VariantProjectionPlan.from(schema, projected, true);
        Assert.assertFalse("fixture must produce a narrowing plan", plan.isEmpty());
        return plan;
    }
}
