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

import static org.apache.asterix.external.util.iceberg.VariantSchemaClipper.LIST;
import static org.apache.asterix.external.util.iceberg.VariantSchemaClipper.METADATA;
import static org.apache.asterix.external.util.iceberg.VariantSchemaClipper.TYPED_VALUE;
import static org.apache.asterix.external.util.iceberg.VariantSchemaClipper.VALUE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.external.input.record.reader.aws.iceberg.VariantProjectedParquetReader;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for reading-shredded (Iceberg VARIANT) pushdown, covering both axes: <b>projection</b> pushdown (read
 * fewer sub-columns, Parts 1–4) and <b>predicate</b> pushdown (read fewer data files, Parts 5–6). The pieces are
 * tested as one pipeline rather than in isolation — each stage is driven by the real output of the stage before it, so
 * a mismatch between them cannot pass.
 * <ul>
 * <li><b>Part 1 — {@link RequestedVariantPaths}</b>: extracts the requested variant sub-path tree from the optimizer's
 * projected {@link ARecordType}. Projected types are built with the same {@link ProjectionFiltrationTypeUtil} the
 * optimizer uses, so the tests exercise the real on-wire representation ({@code ANY} leaves, nested records).</li>
 * <li><b>Part 2 — {@link VariantSchemaClipper}</b>: clips a Parquet variant {@code typed_value} group to those paths.
 * Shredded {@link MessageType}s are hand-built in the same {@code metadata}/{@code value}/{@code typed_value} shape the
 * integration harness verifies on disk, and driven by <em>real</em> {@code RequestedVariantPaths}.</li>
 * <li><b>Part 3 — {@link VariantProjectionPlan}</b>: the gating. A column is pruned only when the flag is on, the
 * <em>Iceberg</em> schema says the column is a {@code VARIANT} (the projected type cannot tell a variant from a
 * struct), and the request actually narrows it.</li>
 * <li><b>Part 4 — end to end on a real shredded Parquet file</b>: the pruned read must return exactly what Iceberg's
 * own unpruned read returns. This is the only test here that proves the clipped schema and the value-reader model
 * still agree once real data flows through them.</li>
 * <li><b>Part 5 — {@link VariantPredicateRewriter}</b>: rewriting a dotted sub-field reference into an Iceberg extract
 * term, structure-preserving over AND/OR/NOT, leaving anything unrewritable untouched rather than dropping it.</li>
 * <li><b>Part 6 — the {@code TableScan} workaround</b>: stripping extract terms from the pushed filter, asserting the
 * strip only ever <em>weakens</em> it (a scan filter may admit more files, never fewer). See the upgrade note
 * below.</li>
 * </ul>
 *
 * <h2>TODO(iceberg-15384): Part 6 contains a deliberate tripwire that fails on upgrade</h2>
 *
 * {@code scanWorkaround_icebergCannotSanitizeExtract} asserts that the Iceberg <em>defect</em> is still present, so it
 * turns red the moment the dependency picks up
 * <a href="https://github.com/apache/iceberg/pull/15384">apache/iceberg#15384</a> — merged upstream <b>and</b>
 * included in the Iceberg release this build depends on. <b>That failure is the feature, not a regression:</b> it is
 * the signal to delete the workarounds, and this test with them. The authoritative removal checklist lives on
 * {@link VariantBoundsEvaluator}'s class javadoc; every site carries the marker {@code iceberg-15384}.
 * <p>
 * Parts 1–4 are unaffected by that upgrade — column projection is a separate Iceberg gap that #15384 does not address.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Requested-path extraction and Parquet typed_value clipping tests "
        + "across nesting, residual-only, multi-column, arrays, scalars, ids, ordering, and safe no-ops")
public class ReadingShreddedPushdownTest {

    private static final String COLUMN = "variant_field";
    /** Rows in the bytes-read fixture: enough that column data dominates the file footer. */
    private static final int WIDE_ROW_COUNT = 500;
    private static final int FAT_VALUE_LENGTH = 200;

    // =====================================================================================================
    // Part 1 — RequestedVariantPaths: extract requested sub-paths from the projected ARecordType
    // =====================================================================================================

    @Test
    public void paths_nullProjection_isAll() {
        Assert.assertTrue(RequestedVariantPaths.fromProjectedType(null, COLUMN).isAll());
    }

    @Test
    public void paths_allFieldsSentinel_isAll() {
        RequestedVariantPaths p =
                RequestedVariantPaths.fromProjectedType(ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, COLUMN);
        Assert.assertTrue(p.isAll());
        Assert.assertTrue("child of all is all", p.child("anything").isAll());
        Assert.assertTrue(p.requestedFieldNames().isEmpty());
    }

    @Test
    public void paths_emptyFieldsSentinel_isAll() {
        Assert.assertTrue(
                RequestedVariantPaths.fromProjectedType(ProjectionFiltrationTypeUtil.EMPTY_TYPE, COLUMN).isAll());
    }

    @Test
    public void paths_columnAbsent_isAll() throws Exception {
        ARecordType projected = projected(List.of(List.of("id"), List.of("other", "x")));
        Assert.assertTrue(RequestedVariantPaths.fromProjectedType(projected, COLUMN).isAll());
    }

    @Test
    public void paths_bareColumn_isAll() throws Exception {
        Assert.assertTrue(pathsFor(List.of(List.of(COLUMN))).isAll());
    }

    @Test
    public void paths_scalarLeaf_isAll() throws Exception {
        ARecordType projected = ProjectionFiltrationTypeUtil
                .getMergedPathRecordType(ProjectionFiltrationTypeUtil.EMPTY_TYPE, List.of(COLUMN), BuiltinType.AINT64);
        Assert.assertTrue(RequestedVariantPaths.fromProjectedType(projected, COLUMN).isAll());
    }

    @Test
    public void paths_singleTopLevel_isNarrowed() throws Exception {
        RequestedVariantPaths p = pathsFor(List.of(List.of(COLUMN, "status")));
        Assert.assertFalse(p.isAll());
        Assert.assertEquals(Set.of("status"), asSet(p.requestedFieldNames()));
        Assert.assertTrue(p.child("status").isAll());
        Assert.assertNull(p.child("missing"));
    }

    @Test
    public void paths_multipleTopLevel_isNarrowed() throws Exception {
        RequestedVariantPaths p = pathsFor(List.of(List.of(COLUMN, "a"), List.of(COLUMN, "b"), List.of(COLUMN, "c")));
        Assert.assertEquals(Set.of("a", "b", "c"), asSet(p.requestedFieldNames()));
        Assert.assertTrue(p.child("a").isAll());
        Assert.assertTrue(p.child("b").isAll());
        Assert.assertTrue(p.child("c").isAll());
    }

    @Test
    public void paths_nested_isNarrowed() throws Exception {
        RequestedVariantPaths p = pathsFor(
                List.of(List.of(COLUMN, "status"), List.of(COLUMN, "objectValue", "nestedString"), List.of("id")));
        Assert.assertEquals(Set.of("status", "objectValue"), asSet(p.requestedFieldNames()));
        Assert.assertTrue(p.child("status").isAll());

        RequestedVariantPaths objectValue = p.child("objectValue");
        Assert.assertNotNull(objectValue);
        Assert.assertFalse(objectValue.isAll());
        Assert.assertEquals(Set.of("nestedString"), asSet(objectValue.requestedFieldNames()));
        Assert.assertTrue(objectValue.child("nestedString").isAll());

        Assert.assertTrue("sibling scalar column is all", RequestedVariantPaths
                .fromProjectedType(projected(List.of(List.of(COLUMN, "status"), List.of("id"))), "id").isAll());
    }

    @Test
    public void paths_siblingSubPathsUnderSameObject_areMerged() throws Exception {
        RequestedVariantPaths p =
                pathsFor(List.of(List.of(COLUMN, "obj", "x"), List.of(COLUMN, "obj", "y"), List.of(COLUMN, "top")));
        Assert.assertEquals(Set.of("obj", "top"), asSet(p.requestedFieldNames()));
        Assert.assertEquals(Set.of("x", "y"), asSet(p.child("obj").requestedFieldNames()));
        Assert.assertTrue(p.child("top").isAll());
    }

    @Test
    public void paths_threeLevelsDeep_isNarrowed() throws Exception {
        RequestedVariantPaths p = pathsFor(List.of(List.of(COLUMN, "a", "b", "c")));
        RequestedVariantPaths a = p.child("a");
        RequestedVariantPaths b = a.child("b");
        Assert.assertEquals(Set.of("a"), asSet(p.requestedFieldNames()));
        Assert.assertEquals(Set.of("b"), asSet(a.requestedFieldNames()));
        Assert.assertEquals(Set.of("c"), asSet(b.requestedFieldNames()));
        Assert.assertTrue(b.child("c").isAll());
    }

    @Test
    public void paths_multipleVariantColumns_extractIndependently() throws Exception {
        ARecordType projected = projected(List.of(List.of("v1", "a"), List.of("v2", "b", "c")));
        RequestedVariantPaths v1 = RequestedVariantPaths.fromProjectedType(projected, "v1");
        RequestedVariantPaths v2 = RequestedVariantPaths.fromProjectedType(projected, "v2");
        Assert.assertEquals(Set.of("a"), asSet(v1.requestedFieldNames()));
        Assert.assertEquals(Set.of("b"), asSet(v2.requestedFieldNames()));
        Assert.assertEquals(Set.of("c"), asSet(v2.child("b").requestedFieldNames()));
    }

    @Test
    public void paths_preserveFieldOrder() {
        // Build the projected record directly to control field order deterministically.
        ARecordType inner = new ARecordType("v", new String[] { "zzz", "aaa", "mmm" },
                new IAType[] { BuiltinType.ANY, BuiltinType.ANY, BuiltinType.ANY }, true);
        ARecordType root = new ARecordType("root", new String[] { COLUMN }, new IAType[] { inner }, true);
        RequestedVariantPaths p = RequestedVariantPaths.fromProjectedType(root, COLUMN);
        Assert.assertEquals(List.of("zzz", "aaa", "mmm"), new ArrayList<>(p.requestedFieldNames()));
    }

    @Test
    public void paths_toString() throws Exception {
        Assert.assertEquals("*", RequestedVariantPaths.all().toString());
        Assert.assertEquals("{status=*}", pathsFor(List.of(List.of(COLUMN, "status"))).toString());
    }

    // =====================================================================================================
    // Part 2 — VariantSchemaClipper: clip the Parquet typed_value group to the requested paths
    // =====================================================================================================

    @Test
    public void clip_keepAll_isNoOp() {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        Assert.assertSame(schema, clip(schema, RequestedVariantPaths.all()));
    }

    @Test
    public void clip_serializedColumn_isNoOp() throws Exception {
        GroupType serialized =
                Types.buildGroup(Repetition.OPTIONAL).addField(bin(METADATA)).addField(bin(VALUE)).named(COLUMN);
        MessageType schema = message(serialized);
        Assert.assertSame(schema, clip(schema, pathsFor(List.of(List.of(COLUMN, "status")))));
    }

    @Test
    public void clip_columnAbsent_isNoOp() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"))));
        // Ask for a column that isn't in the schema.
        Assert.assertSame(schema,
                VariantSchemaClipper.clip(schema, "does_not_exist", pathsFor(List.of(List.of("does_not_exist", "x")))));
    }

    @Test
    public void clip_columnIsPrimitive_isNoOp() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"))));
        // "id" is a primitive column, not a variant group.
        Assert.assertSame(schema, VariantSchemaClipper.clip(schema, "id", pathsFor(List.of(List.of("id", "x")))));
    }

    @Test
    public void clip_requestingExactlyAllMembers_isNoOp() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        // Every shredded member is requested (each whole) -> nothing to prune -> same instance.
        Assert.assertSame(schema, clip(schema, pathsFor(List.of(List.of(COLUMN, "status"), List.of(COLUMN, "other")))));
    }

    @Test
    public void clip_topLevelNarrow_keepsRequestedPlusMetadataValue() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"), scalar("third"))));
        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "status"))));
        Assert.assertTrue(variant.containsField(METADATA));
        Assert.assertTrue(variant.containsField(VALUE));
        Assert.assertEquals(Set.of("status"), typedValueFieldNames(variant));
    }

    @Test
    public void clip_topLevelKeepMultiple() throws Exception {
        MessageType schema =
                message(variant(objectTyped(scalar("a"), scalar("b"), scalar("c"), scalar("d"), scalar("e"))));
        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "b"), List.of(COLUMN, "d"))));
        Assert.assertEquals(Set.of("b", "d"), typedValueFieldNames(variant));
    }

    @Test
    public void clip_nested_prunesDeep() throws Exception {
        GroupType objectValue = object("objectValue", scalar("nestedString"), scalar("nestedInt"));
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("dropMe"), objectValue)));
        GroupType variant = clippedVariant(schema,
                pathsFor(List.of(List.of(COLUMN, "status"), List.of(COLUMN, "objectValue", "nestedString"))));

        Assert.assertEquals(Set.of("status", "objectValue"), typedValueFieldNames(variant));
        GroupType clippedObj = variant.getType(TYPED_VALUE).asGroupType().getType("objectValue").asGroupType();
        Assert.assertTrue("nested value retained", clippedObj.containsField(VALUE));
        Assert.assertEquals(Set.of("nestedString"), asSet(fieldNames(clippedObj.getType(TYPED_VALUE).asGroupType())));
    }

    @Test
    public void clip_threeLevelsDeep_prunes() throws Exception {
        GroupType level3 = object("c", scalar("d"), scalar("e"));
        GroupType level2 = object("b", level3, scalar("bSibling"));
        GroupType level1 = object("a", level2, scalar("aSibling"));
        MessageType schema = message(variant(objectTyped(level1, scalar("topSibling"))));

        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "a", "b", "c", "d"))));
        GroupType a = variant.getType(TYPED_VALUE).asGroupType().getType("a").asGroupType();
        GroupType b = a.getType(TYPED_VALUE).asGroupType().getType("b").asGroupType();
        GroupType c = b.getType(TYPED_VALUE).asGroupType().getType("c").asGroupType();

        Assert.assertEquals(Set.of("a"), typedValueFieldNames(variant)); // topSibling dropped
        Assert.assertEquals(Set.of("b"), asSet(fieldNames(a.getType(TYPED_VALUE).asGroupType()))); // aSibling dropped
        Assert.assertEquals(Set.of("c"), asSet(fieldNames(b.getType(TYPED_VALUE).asGroupType()))); // bSibling dropped
        Assert.assertEquals(Set.of("d"), asSet(fieldNames(c.getType(TYPED_VALUE).asGroupType()))); // e dropped
    }

    @Test
    public void clip_shreddedAndResidualTogether_keepsOnlyShreddedMember() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        // Request one shredded field (status) and one that isn't shredded here (residualX).
        GroupType variant =
                clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "status"), List.of(COLUMN, "residualX"))));
        Assert.assertEquals("only the shredded member survives; residualX comes from value", Set.of("status"),
                typedValueFieldNames(variant));
        Assert.assertTrue(variant.containsField(VALUE));
    }

    @Test
    public void clip_residualOnlyRequest_dropsTypedValueButKeepsMetadataValue() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "residualOnly"))));
        Assert.assertTrue(variant.containsField(METADATA));
        Assert.assertTrue(variant.containsField(VALUE));
        Assert.assertFalse("all requested fields residual -> typed_value dropped", variant.containsField(TYPED_VALUE));
    }

    @Test
    public void clip_nestedResidualOnly_dropsNestedTypedValueKeepsNestedValue() throws Exception {
        GroupType objectValue = object("objectValue", scalar("nestedString"), scalar("nestedInt"));
        MessageType schema = message(variant(objectTyped(objectValue)));
        // Ask for objectValue.residualZ, which isn't a shredded member of objectValue.
        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "objectValue", "residualZ"))));
        Assert.assertEquals(Set.of("objectValue"), typedValueFieldNames(variant));
        GroupType clippedObj = variant.getType(TYPED_VALUE).asGroupType().getType("objectValue").asGroupType();
        Assert.assertTrue("nested value retained", clippedObj.containsField(VALUE));
        Assert.assertFalse("nested typed_value dropped", clippedObj.containsField(TYPED_VALUE));
    }

    @Test
    public void clip_nestedPathIntoScalarField_keepsScalarWhole() throws Exception {
        // status is a scalar shredded field; a query path status.foo can't sub-select it -> keep the whole field.
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        GroupType variant = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "status", "foo"))));
        Assert.assertEquals(Set.of("status"), typedValueFieldNames(variant));
        GroupType status = variant.getType(TYPED_VALUE).asGroupType().getType("status").asGroupType();
        Assert.assertTrue(status.containsField(VALUE));
        Assert.assertTrue("scalar field's typed_value kept whole", status.containsField(TYPED_VALUE));
    }

    @Test
    public void clip_arrayTypedValue_isNoOp() throws Exception {
        GroupType arrayTyped = Types.buildGroup(Repetition.OPTIONAL).addField(bin(LIST)).named(TYPED_VALUE);
        GroupType variant = Types.buildGroup(Repetition.OPTIONAL).addField(bin(METADATA)).addField(bin(VALUE))
                .addField(arrayTyped).named(COLUMN);
        MessageType schema = message(variant);
        Assert.assertSame(schema, clip(schema, pathsFor(List.of(List.of(COLUMN, "status")))));
    }

    @Test
    public void clip_scalarTypedValue_isNoOp() throws Exception {
        // A top-level scalar variant: typed_value is a primitive, not an object group.
        GroupType variant = Types.buildGroup(Repetition.OPTIONAL).addField(bin(METADATA)).addField(bin(VALUE))
                .addField(bin(TYPED_VALUE)).named(COLUMN);
        MessageType schema = message(variant);
        Assert.assertSame(schema, clip(schema, pathsFor(List.of(List.of(COLUMN, "status")))));
    }

    @Test
    public void clip_otherColumnsUntouched() throws Exception {
        MessageType schema = message(variant(objectTyped(scalar("status"), scalar("other"))));
        MessageType clipped = clip(schema, pathsFor(List.of(List.of(COLUMN, "status"))));
        // The sibling primitive column survives verbatim.
        Assert.assertTrue(clipped.containsField("id"));
        Assert.assertEquals(schema.getType("id"), clipped.getType("id"));
    }

    @Test
    public void clip_onlyTargetVariantColumnIsClipped() throws Exception {
        GroupType v1 = variantNamed("v1", objectTyped(scalar("a"), scalar("b")));
        GroupType v2 = variantNamed("v2", objectTyped(scalar("c"), scalar("d")));
        MessageType schema = new MessageType("table", List.of(v1, v2));

        MessageType clipped = VariantSchemaClipper.clip(schema, "v1",
                RequestedVariantPaths.fromProjectedType(projected(List.of(List.of("v1", "a"))), "v1"));

        Assert.assertEquals("v1 narrowed", Set.of("a"),
                asSet(fieldNames(clipped.getType("v1").asGroupType().getType(TYPED_VALUE).asGroupType())));
        Assert.assertEquals("v2 untouched", schema.getType("v2"), clipped.getType("v2"));
    }

    @Test
    public void clip_fieldIdsPreserved_topAndNested() throws Exception {
        GroupType inner = object("objectValue", scalar("nestedString"), scalar("nestedInt")).withId(42);
        GroupType typed = objectTyped(scalar("status"), inner).withId(99);
        GroupType variant = Types.buildGroup(Repetition.OPTIONAL).addField(bin(METADATA)).addField(bin(VALUE))
                .addField(typed).named(COLUMN).withId(7);
        MessageType schema = message(variant);

        GroupType clipped = clippedVariant(schema, pathsFor(List.of(List.of(COLUMN, "objectValue", "nestedString"))));
        Assert.assertEquals(7, clipped.getId().intValue());
        Assert.assertEquals(99, clipped.getType(TYPED_VALUE).getId().intValue());
        Assert.assertEquals(42, clipped.getType(TYPED_VALUE).asGroupType().getType("objectValue").getId().intValue());
    }

    // =====================================================================================================
    // Part 3 — VariantProjectionPlan: gate on the flag + the Iceberg VARIANT type, per column
    // =====================================================================================================

    @Test
    public void plan_disabled_isEmpty() throws Exception {
        // Even with a narrowing request, a disabled flag yields no plan.
        VariantProjectionPlan plan =
                VariantProjectionPlan.from(icebergSchema(), projected(List.of(List.of(COLUMN, "status"))), false);
        Assert.assertTrue(plan.isEmpty());
    }

    @Test
    public void plan_nullSchema_isEmpty() throws Exception {
        Assert.assertTrue(
                VariantProjectionPlan.from(null, projected(List.of(List.of(COLUMN, "status"))), true).isEmpty());
    }

    @Test
    public void plan_variantNarrowed_isPresent() throws Exception {
        VariantProjectionPlan plan =
                VariantProjectionPlan.from(icebergSchema(), projected(List.of(List.of(COLUMN, "status"))), true);
        Assert.assertFalse(plan.isEmpty());
        Assert.assertEquals(Set.of(COLUMN), asSet(plan.columns()));
        RequestedVariantPaths paths = plan.get(COLUMN);
        Assert.assertNotNull(paths);
        Assert.assertEquals(Set.of("status"), asSet(paths.requestedFieldNames()));
        Assert.assertNull("non-variant column absent from plan", plan.get("id"));
    }

    @Test
    public void plan_wholeVariant_isSkipped() throws Exception {
        // Bare variant column -> paths is "all" -> nothing to prune -> not in the plan.
        Assert.assertTrue(
                VariantProjectionPlan.from(icebergSchema(), projected(List.of(List.of(COLUMN))), true).isEmpty());
    }

    @Test
    public void plan_primitiveColumn_isIgnored() throws Exception {
        Assert.assertTrue(
                VariantProjectionPlan.from(icebergSchema(), projected(List.of(List.of("id"))), true).isEmpty());
    }

    @Test
    public void plan_structColumn_isNotRoutedHere() throws Exception {
        // a_struct.x is a real Iceberg struct path -> handled by Iceberg's own pruning, never entered into the plan.
        Assert.assertTrue(VariantProjectionPlan
                .from(icebergSchema(), projected(List.of(List.of("a_struct", "x"))), true).isEmpty());
    }

    @Test
    public void plan_mixedVariantAndStruct_onlyVariantIncluded() throws Exception {
        VariantProjectionPlan plan = VariantProjectionPlan.from(icebergSchema(),
                projected(List.of(List.of(COLUMN, "status"), List.of("a_struct", "x"))), true);
        Assert.assertEquals("only the variant column is planned", Set.of(COLUMN), asSet(plan.columns()));
    }

    @Test
    public void plan_multipleVariantColumns_allIncluded() throws Exception {
        VariantProjectionPlan plan = VariantProjectionPlan.from(icebergSchema(),
                projected(List.of(List.of(COLUMN, "status"), List.of("v2", "y"))), true);
        Assert.assertEquals(Set.of(COLUMN, "v2"), asSet(plan.columns()));
        Assert.assertEquals(Set.of("status"), asSet(plan.get(COLUMN).requestedFieldNames()));
        Assert.assertEquals(Set.of("y"), asSet(plan.get("v2").requestedFieldNames()));
    }

    /** id (primitive) + two VARIANT columns + a real STRUCT column, to exercise the type gating.
     * A variant sub-field may itself be named with a dot, and the filter builder's reference name cannot express the
     * difference: {@code "variant_field.a.b"} is what both a field named {@code "a.b"} and a nested {@code a -> b}
     * produce. Rewriting on the wrong reading prunes against the wrong sub-field's bounds and drops matching rows, so
     * the decision is made from the unjoined segments the builder recorded, never by splitting the name.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins the segment-aware rewrite contract: nesting is pushed, a dotted field name is declined, and unknown or collided names are declined rather than guessed")
    @Test
    public void rewrite_usesUnjoinedSegmentsAndNeverGuessesADottedName() {
        Schema schema = icebergSchema();
        String name = COLUMN + ".a.b";
        org.apache.iceberg.expressions.Expression predicate = org.apache.iceberg.expressions.Expressions.equal(name, 5);

        // Nesting: a -> b. Safe to express as $.a.b, so it is rewritten to an extract term and can prune.
        org.apache.iceberg.expressions.Expression nested = VariantPredicateRewriter.rewrite(predicate, schema,
                java.util.Map.of(name, java.util.List.of(COLUMN, "a", "b")));
        Assert.assertNotSame("nesting must still be rewritten, or we lose pushdown we already had", predicate, nested);
        Assert.assertTrue("expected an extract term, got: " + nested, nested.toString().contains("$.a.b"));

        // One field literally named "a.b". Dot notation cannot express it, so it must NOT be rewritten.
        Assert.assertSame("a sub-field named \"a.b\" must not be rewritten as nesting", predicate,
                VariantPredicateRewriter.rewrite(predicate, schema,
                        java.util.Map.of(name, java.util.List.of(COLUMN, "a.b"))));

        // Name absent from the map: the segments are unknown, so the two readings cannot be told apart.
        Assert.assertSame("an unrecorded reference must not be rewritten", predicate,
                VariantPredicateRewriter.rewrite(predicate, schema, java.util.Map.of()));

        // Empty list = two different paths collided on this name in one query.
        Assert.assertSame("a name marked ambiguous must not be rewritten", predicate,
                VariantPredicateRewriter.rewrite(predicate, schema, java.util.Map.of(name, java.util.List.of())));
    }

    /** Segments needing RFC 9535 escaping cannot be written after a dot, so they are declined rather than mangled. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Checks the RFC 9535 member-name-shorthand gate that mirrors Iceberg's own PathUtil rule")
    @Test
    public void rewrite_declinesSegmentsThatNeedEscaping() {
        Schema schema = icebergSchema();
        for (String odd : new String[] { "a.b", "a b", "a-b", "a'b", "a[0]", "", "1a" }) {
            String name = COLUMN + "." + odd;
            org.apache.iceberg.expressions.Expression predicate =
                    org.apache.iceberg.expressions.Expressions.equal(name, 5);
            Assert.assertSame("segment needing escaping must not be rewritten: '" + odd + "'", predicate,
                    VariantPredicateRewriter.rewrite(predicate, schema,
                            java.util.Map.of(name, java.util.List.of(COLUMN, odd))));
        }
    }

    /**
     * A variant does not have to be a top-level column. When it sits inside a struct, the path's first segment is the
     * struct, not the variant, so the rewrite has to find the variant by walking prefixes — {@code st}, then
     * {@code st.v} — and treat only what follows it as the variant sub-path.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins that a variant nested in a struct is rewritten against column st.v with sub-path $.bucket, rather than declined because the first segment is a struct")
    @Test
    public void rewrite_findsAVariantNestedInsideAStruct() {
        Schema nested =
                new Schema(
                        org.apache.iceberg.types.Types.NestedField.required(1, "id",
                                org.apache.iceberg.types.Types.IntegerType
                                        .get()),
                        org.apache.iceberg.types.Types.NestedField.optional(2, "st",
                                org.apache.iceberg.types.Types.StructType.of(
                                        org.apache.iceberg.types.Types.NestedField.optional(3, "v",
                                                org.apache.iceberg.types.Types.VariantType.get()),
                                        org.apache.iceberg.types.Types.NestedField.optional(4, "label",
                                                org.apache.iceberg.types.Types.StringType.get()))));
        String name = "st.v.bucket";
        org.apache.iceberg.expressions.Expression predicate =
                org.apache.iceberg.expressions.Expressions.lessThan(name, 2);

        org.apache.iceberg.expressions.Expression rewritten = VariantPredicateRewriter.rewrite(predicate, nested,
                java.util.Map.of(name, java.util.List.of("st", "v", "bucket")));
        Assert.assertNotSame("a variant inside a struct must still be rewritten", predicate, rewritten);
        String rendered = rewritten.toString();
        Assert.assertTrue("the extract's column must be the variant itself, st.v — got: " + rendered,
                rendered.contains("st.v"));
        Assert.assertTrue("the sub-path must start AT the variant, not include the struct — got: " + rendered,
                rendered.contains("$.bucket"));

        // The ordinary sibling of the variant is a real struct field and must be left for Iceberg to handle.
        org.apache.iceberg.expressions.Expression sibling =
                org.apache.iceberg.expressions.Expressions.equal("st.label", "L2");
        Assert.assertSame("a plain struct field must not be rewritten", sibling, VariantPredicateRewriter
                .rewrite(sibling, nested, java.util.Map.of("st.label", java.util.List.of("st", "label"))));
    }

    /**
     * A variant can also sit inside a list or a map, and neither is handled: projection pushdown never sees them
     * (the plan only walks top-level columns) and the rewrite refuses to resolve a path through them.
     * <p>
     * The rewrite's refusal is deliberate rather than incidental. Iceberg's {@code findField} resolves
     * {@code "arr.element"} and {@code "m.value"} perfectly well, so a prefix walk that only asked "is this a variant?"
     * would happily rewrite {@code arr.element.x = 1} into an extract term — and bounds for values inside an array
     * describe <em>all</em> elements, which carries existential semantics this evaluator does not model. Following
     * struct nesting only keeps those on the ordinary, unpruned path.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins that variants inside a list or map are excluded from both projection and predicate pushdown, so the struct-only walk cannot silently start resolving through collection boundaries")
    @Test
    public void variantInsideAListOrMap_isLeftAlone() throws Exception {
        Schema inList = new Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "arr", org.apache.iceberg.types.Types.ListType
                        .ofOptional(3, org.apache.iceberg.types.Types.VariantType.get())));
        Schema inMap = new Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "m",
                        org.apache.iceberg.types.Types.MapType.ofOptional(3, 4,
                                org.apache.iceberg.types.Types.StringType.get(),
                                org.apache.iceberg.types.Types.VariantType.get())));

        ARecordType projected = ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of("arr", "x")));
        Assert.assertTrue("a variant in a list must never enter the projection plan",
                VariantProjectionPlan.from(inList, projected, true).isEmpty());
        Assert.assertTrue("a variant in a map must never enter the projection plan",
                VariantProjectionPlan.from(inMap, projected, true).isEmpty());

        assertDeclined(inList, "arr.element.x", List.of("arr", "element", "x"));
        assertDeclined(inList, "arr.x", List.of("arr", "x"));
        assertDeclined(inMap, "m.value.x", List.of("m", "value", "x"));
    }

    private static void assertDeclined(Schema schema, String name, List<String> segments) {
        org.apache.iceberg.expressions.Expression predicate = org.apache.iceberg.expressions.Expressions.equal(name, 1);
        Assert.assertSame("must not be rewritten: " + name, predicate,
                VariantPredicateRewriter.rewrite(predicate, schema, java.util.Map.of(name, segments)));
    }

    /**
     * Nesting depth of the unreferenced sub-field in the deep-pruning test. 500 is deliberate: {@code variantDepth}
     * accepts up to 1000, so this is effectively the deepest variant the product will read, which makes the test a
     * boundary check as well as a performance one — the clipper recurses once per level, and a stack overflow there
     * would NOT be caught by the reader's {@code catch (Exception)} fallback.
     */
    private static final int DEEP_DEPTH = 500;
    /** Rows in the deep-pruning fixture. */
    private static final int DEEP_ROW_COUNT = 1000;

    /**
     * The cost of NOT pruning, made visible: a variant with one cheap sub-field and one nested a hundred levels deep.
     * <p>
     * The sibling bytes-read test uses a wide variant, where the saving is bulk. This one is about shape: the deep
     * chain becomes a hundred nested Parquet groups, so reading it costs column chunks and assembly work per row even
     * though the leaf data is tiny. A query touching only {@code x} should never pay for any of it.
     * <p>
     * <b>What is asserted, and why not bytes or time.</b> The guard is that the deep chain never comes back in the
     * reconstructed value: if it were being read, it would be there. Neither measurement is asserted, for opposite
     * reasons, and both are logged instead:
     * <ul>
     * <li><b>Bytes barely move</b> — measured at 9.05 MB pruned against 9.10 MB full, under 1% apart. At this depth the
     * Parquet footer IS the file, and both reads must parse all of it. Depth is not where I/O is saved; the sibling
     * wide-variant test is where that is asserted, and there the pruned read fetches less than half.</li>
     * <li><b>Time moves a lot</b> — 274 ms pruned against 2388 ms full, roughly 8.7x, because the cost of a deep field
     * is per-row assembly of a thousand nested groups rather than the bytes it occupies. That is the finding worth
     * recording, but a wall-clock ratio is exactly the assertion that passes on a developer machine and flakes on a
     * loaded build agent, so it is printed rather than asserted.</li>
     * </ul>
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Deeply nested unreferenced sub-field (100 levels, 1000 rows) so the cost of reading it dominates; asserts the pruned read fetches far fewer bytes and reports both timings")
    @Test
    public void endToEnd_prunedReadSkipsADeeplyNestedSubField() throws Exception {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()));
        List<String> names = new ArrayList<>();
        names.add("x");
        for (int k = 1; k <= DEEP_DEPTH; k++) {
            names.add("d" + k);
        }
        org.apache.iceberg.variants.VariantMetadata meta =
                org.apache.iceberg.variants.Variants.metadata(names.toArray(new String[0]));

        java.io.File dir = java.nio.file.Files.createTempDirectory("shredded-pushdown-deep").toFile();
        java.io.File dataFile = new java.io.File(dir, "deep.parquet");
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, deepVariant(meta, 0).value());

        org.apache.iceberg.data.GenericRecord template = org.apache.iceberg.data.GenericRecord.create(schema);
        try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> writer =
                org.apache.iceberg.parquet.Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
                        .variantShreddingFunc((fieldId, name) -> typedValue).build()) {
            for (int i = 0; i < DEEP_ROW_COUNT; i++) {
                org.apache.iceberg.data.Record row = template.copy();
                row.setField("id", i);
                row.setField(COLUMN, deepVariant(meta, i));
                writer.add(row);
            }
        }

        CountingInputFile baselineFile = new CountingInputFile(org.apache.iceberg.Files.localInput(dataFile));
        long baselineStart = System.nanoTime();
        int baselineRows = 0;
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> it =
                org.apache.iceberg.parquet.Parquet.read(baselineFile).project(schema)
                        .createReaderFunc(
                                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(schema, fs))
                        .build()) {
            for (org.apache.iceberg.data.Record ignored : it) {
                baselineRows++;
            }
        }
        long baselineMillis = (System.nanoTime() - baselineStart) / 1_000_000;

        VariantProjectionPlan plan = VariantProjectionPlan.from(schema, projected(List.of(List.of(COLUMN, "x"))), true);
        Assert.assertFalse("plan must narrow the variant column", plan.isEmpty());
        CountingInputFile prunedFile = new CountingInputFile(org.apache.iceberg.Files.localInput(dataFile));
        long prunedStart = System.nanoTime();
        int prunedRows = 0;
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(prunedFile, schema, null, 0, 0, true, plan)) {
            Assert.assertTrue("clip must actually prune this file", reader.canPrune());
            for (org.apache.iceberg.data.Record record : reader) {
                org.apache.iceberg.variants.VariantValue value =
                        ((org.apache.iceberg.variants.Variant) record.getField(COLUMN)).value();
                Assert.assertNotNull("the requested sub-field must survive", value.asObject().get("x"));
                Assert.assertNull("the deep chain must not be read at all", value.asObject().get("d1"));
                prunedRows++;
            }
        }
        long prunedMillis = (System.nanoTime() - prunedStart) / 1_000_000;

        Assert.assertEquals("both reads must see every row", DEEP_ROW_COUNT, baselineRows);
        Assert.assertEquals("pruning must not change the row count", baselineRows, prunedRows);
        long baselineBytes = baselineFile.bytesRead();
        long prunedBytes = prunedFile.bytesRead();
        Assert.assertTrue("both reads must have read something", baselineBytes > 0 && prunedBytes > 0);
        // Deliberately stdout rather than the logger: the measurement is the point of this test, and it has to be
        // visible in ordinary build output rather than only when debug logging happens to be on.
        System.out.printf(
                "deep variant (%d levels, %d rows): full read %d bytes in %d ms, pruned read %d bytes in " + "%d ms%n",
                DEEP_DEPTH, DEEP_ROW_COUNT, baselineBytes, baselineMillis, prunedBytes, prunedMillis);
    }

    /** @return {@code { x: i, d1: { d2: { ... { d100: "leaf-i" } } } }} — one cheap field and one very deep one. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Builds the deep fixture row; the leaf varies per row so the deep columns cannot be dictionary-collapsed to nothing")
    private static org.apache.iceberg.variants.Variant deepVariant(org.apache.iceberg.variants.VariantMetadata meta,
            int i) {
        org.apache.iceberg.variants.VariantValue value = org.apache.iceberg.variants.Variants.of("leaf-" + i);
        for (int k = DEEP_DEPTH; k >= 2; k--) {
            org.apache.iceberg.variants.ShreddedObject level = org.apache.iceberg.variants.Variants.object(meta);
            level.put("d" + k, value);
            value = level;
        }
        org.apache.iceberg.variants.ShreddedObject top = org.apache.iceberg.variants.Variants.object(meta);
        top.put("x", org.apache.iceberg.variants.Variants.of(i));
        top.put("d1", value);
        return org.apache.iceberg.variants.Variant.of(meta, top);
    }

    private static Schema icebergSchema() {
        return new Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "v2",
                        org.apache.iceberg.types.Types.VariantType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(4, "a_struct",
                        org.apache.iceberg.types.Types.StructType.of(org.apache.iceberg.types.Types.NestedField
                                .optional(5, "x", org.apache.iceberg.types.Types.StringType.get()))));
    }

    // =====================================================================================================
    // Part 4 — end-to-end on a REAL shredded Parquet file: the pruned read must agree with Iceberg's own read
    // =====================================================================================================

    /**
     * Writes a genuinely shredded Parquet file to a temp dir, then reads it twice: once through Iceberg's standard
     * {@code Parquet.read()} (the trusted baseline) and once through {@link VariantProjectedParquetReader} with a plan
     * narrowing to a subset of paths. Every requested path must carry identical values in both reads, and the pruned
     * paths must be absent from the narrowed read — proving the clip both preserves the requested data and actually
     * removed sub-columns. No Docker/S3/Nessie needed.
     */
    @Test
    public void endToEnd_prunedReadMatchesStandardRead() throws Exception {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()));

        java.io.File dir = java.nio.file.Files.createTempDirectory("shredded-pushdown").toFile();
        java.io.File dataFile = new java.io.File(dir, "shredded.parquet");
        Assert.assertTrue("temp data file must not pre-exist", !dataFile.exists() || dataFile.delete());

        org.apache.iceberg.variants.VariantMetadata meta = org.apache.iceberg.variants.Variants.metadata("keptScalar",
                "droppedScalar", "obj", "innerKept", "innerDropped");
        org.apache.iceberg.variants.ShreddedObject inner = org.apache.iceberg.variants.Variants.object(meta);
        inner.put("innerKept", org.apache.iceberg.variants.Variants.of("inner-kept"));
        inner.put("innerDropped", org.apache.iceberg.variants.Variants.of(7));
        org.apache.iceberg.variants.ShreddedObject root = org.apache.iceberg.variants.Variants.object(meta);
        root.put("keptScalar", org.apache.iceberg.variants.Variants.of("kept"));
        root.put("droppedScalar", org.apache.iceberg.variants.Variants.of(123));
        root.put("obj", inner);
        org.apache.iceberg.variants.Variant variant = org.apache.iceberg.variants.Variant.of(meta, root);

        // Fully shred the value so every field becomes a typed sub-column (canonical typed_value from Iceberg itself).
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, variant.value());
        org.apache.iceberg.data.GenericRecord template = org.apache.iceberg.data.GenericRecord.create(schema);
        org.apache.iceberg.data.Record row = template.copy();
        row.setField("id", 1);
        row.setField(COLUMN, variant);
        try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> writer =
                org.apache.iceberg.parquet.Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
                        .variantShreddingFunc((fieldId, name) -> typedValue).build()) {
            writer.add(row);
        }

        // Sanity: the file really is shredded (otherwise this test would silently prove nothing).
        try (org.apache.parquet.hadoop.ParquetFileReader footer = org.apache.parquet.hadoop.ParquetFileReader.open(
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(dataFile.toURI()),
                        new org.apache.hadoop.conf.Configuration()))) {
            GroupType variantGroup = footer.getFooter().getFileMetaData().getSchema().getType(COLUMN).asGroupType();
            Assert.assertTrue("fixture must be shredded", variantGroup.containsField(TYPED_VALUE));
        }

        org.apache.iceberg.io.InputFile in = org.apache.iceberg.Files.localInput(dataFile);

        // Baseline: Iceberg's own read of the whole variant.
        org.apache.iceberg.variants.VariantValue baseline;
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> it =
                org.apache.iceberg.parquet.Parquet.read(in).project(schema)
                        .createReaderFunc(
                                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(schema, fs))
                        .build()) {
            java.util.Iterator<org.apache.iceberg.data.Record> iter = it.iterator();
            Assert.assertTrue(iter.hasNext());
            baseline = ((org.apache.iceberg.variants.Variant) iter.next().getField(COLUMN)).value();
        }

        // Narrowed read: request only keptScalar and obj.innerKept.
        VariantProjectionPlan plan = VariantProjectionPlan.from(schema,
                projected(List.of(List.of(COLUMN, "keptScalar"), List.of(COLUMN, "obj", "innerKept"))), true);
        Assert.assertFalse("plan must narrow the variant column", plan.isEmpty());

        org.apache.iceberg.variants.VariantValue narrowed;
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(in, schema, null, 0, 0, true, plan)) {
            Assert.assertTrue("clip must actually prune this file", reader.canPrune());
            java.util.Iterator<org.apache.iceberg.data.Record> iter = reader.iterator();
            Assert.assertTrue(iter.hasNext());
            narrowed = ((org.apache.iceberg.variants.Variant) iter.next().getField(COLUMN)).value();
            Assert.assertFalse("exactly one row", iter.hasNext());
        }

        // Requested paths agree with the baseline.
        Assert.assertEquals(baseline.asObject().get("keptScalar").asPrimitive().get(),
                narrowed.asObject().get("keptScalar").asPrimitive().get());
        Assert.assertEquals(baseline.asObject().get("obj").asObject().get("innerKept").asPrimitive().get(),
                narrowed.asObject().get("obj").asObject().get("innerKept").asPrimitive().get());

        // Unreferenced sub-columns were pruned (present in the baseline, gone from the narrowed read).
        Assert.assertNotNull("baseline has the dropped scalar", baseline.asObject().get("droppedScalar"));
        Assert.assertNull("pruned scalar must be absent", narrowed.asObject().get("droppedScalar"));
        Assert.assertNotNull("baseline has the dropped nested field",
                baseline.asObject().get("obj").asObject().get("innerDropped"));
        Assert.assertNull("pruned nested field must be absent",
                narrowed.asObject().get("obj").asObject().get("innerDropped"));
    }

    /**
     * Pruning must actually reduce I/O, not merely produce the right values. Both reads go through the same counting
     * {@link org.apache.iceberg.io.InputFile}, so this compares bytes genuinely pulled from the file rather than
     * column sizes inferred from the footer — a reader that requested a narrow schema but still fetched whole row
     * groups would be caught here.
     * <p>
     * <b>This is the only assertion anywhere that variant projection pushdown reduces reads.</b> No row-level test can
     * see it: pruning that silently no-ops still returns exactly the right answers, and
     * {@code processedObjects} counts rows, so the cluster suites are blind to it too.
     * <p>
     * Two things about the fixture are deliberate. It is lopsided — three fat string sub-columns the query never
     * references against one small one it does — and the fat values are <em>distinct per row</em>. Repeating one
     * value would let dictionary encoding collapse those columns to almost nothing, leaving no bytes to save and a
     * test that passes for the wrong reason. Enough rows are written that column data dominates the footer, which a
     * single-row fixture cannot do.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Counts bytes actually read through a wrapping InputFile for the pruned vs unpruned read of the same shredded file; the only evidence that variant projection pushdown reduces I/O rather than just returning correct values")
    @Test
    public void endToEnd_prunedReadFetchesFewerBytes() throws Exception {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()));

        java.io.File dir = java.nio.file.Files.createTempDirectory("shredded-pushdown-bytes").toFile();
        java.io.File dataFile = new java.io.File(dir, "wide.parquet");
        Assert.assertTrue("temp data file must not pre-exist", !dataFile.exists() || dataFile.delete());

        org.apache.iceberg.variants.VariantMetadata meta =
                org.apache.iceberg.variants.Variants.metadata("kept", "fat1", "fat2", "fat3");
        // Row 0's shape defines the shredding schema; every row shreds the same way.
        org.apache.iceberg.variants.Variant shape = wideVariant(meta, 0);
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, shape.value());

        org.apache.iceberg.data.GenericRecord template = org.apache.iceberg.data.GenericRecord.create(schema);
        try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> writer =
                org.apache.iceberg.parquet.Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
                        .variantShreddingFunc((fieldId, name) -> typedValue).build()) {
            for (int i = 0; i < WIDE_ROW_COUNT; i++) {
                org.apache.iceberg.data.Record row = template.copy();
                row.setField("id", i);
                row.setField(COLUMN, wideVariant(meta, i));
                writer.add(row);
            }
        }

        // Baseline: Iceberg's own read of the whole variant, through a counting file.
        CountingInputFile baselineFile = new CountingInputFile(org.apache.iceberg.Files.localInput(dataFile));
        int baselineRows = 0;
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> it =
                org.apache.iceberg.parquet.Parquet.read(baselineFile).project(schema)
                        .createReaderFunc(
                                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(schema, fs))
                        .build()) {
            for (org.apache.iceberg.data.Record ignored : it) {
                baselineRows++;
            }
        }

        // Pruned: only the small "kept" sub-column is requested.
        VariantProjectionPlan plan =
                VariantProjectionPlan.from(schema, projected(List.of(List.of(COLUMN, "kept"))), true);
        Assert.assertFalse("plan must narrow the variant column", plan.isEmpty());

        CountingInputFile prunedFile = new CountingInputFile(org.apache.iceberg.Files.localInput(dataFile));
        int prunedRows = 0;
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(prunedFile, schema, null, 0, 0, true, plan)) {
            Assert.assertTrue("clip must actually prune this file", reader.canPrune());
            for (org.apache.iceberg.data.Record record : reader) {
                org.apache.iceberg.variants.VariantValue value =
                        ((org.apache.iceberg.variants.Variant) record.getField(COLUMN)).value();
                // The requested sub-field must survive, and the unreferenced fat ones must be gone.
                Assert.assertNotNull("requested sub-field must be present", value.asObject().get("kept"));
                Assert.assertNull("unreferenced sub-field must be pruned", value.asObject().get("fat1"));
                prunedRows++;
            }
        }

        Assert.assertEquals("both reads must see every row", WIDE_ROW_COUNT, baselineRows);
        Assert.assertEquals("pruning must not change the row count", baselineRows, prunedRows);

        long baselineBytes = baselineFile.bytesRead();
        long prunedBytes = prunedFile.bytesRead();
        Assert.assertTrue("both reads must have read something", baselineBytes > 0 && prunedBytes > 0);
        // Deliberately a loose ratio rather than an exact byte count: encoding and compression details shift between
        // Parquet versions, but dropping three fat columns of four can never land anywhere near half.
        Assert.assertTrue("pruned read must fetch far fewer bytes than the full read, got pruned=" + prunedBytes
                + " baseline=" + baselineBytes, prunedBytes * 2 < baselineBytes);
    }

    /** Row {@code i}'s variant: one small low-cardinality field plus three fat ones, all distinct per row. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Builds the lopsided fixture row for the bytes-read test; fat values vary per row so dictionary encoding cannot collapse them")
    private static org.apache.iceberg.variants.Variant wideVariant(org.apache.iceberg.variants.VariantMetadata meta,
            int i) {
        org.apache.iceberg.variants.ShreddedObject root = org.apache.iceberg.variants.Variants.object(meta);
        // Low cardinality, so this column stays tiny and the saving comes from the fat ones.
        root.put("kept", org.apache.iceberg.variants.Variants.of("k" + (i % 5)));
        root.put("fat1", org.apache.iceberg.variants.Variants.of(fatValue(1, i)));
        root.put("fat2", org.apache.iceberg.variants.Variants.of(fatValue(2, i)));
        root.put("fat3", org.apache.iceberg.variants.Variants.of(fatValue(3, i)));
        return org.apache.iceberg.variants.Variant.of(meta, root);
    }

    /**
     * A long, deterministic but incompressible-looking value. Both properties matter: repeating a short token would
     * let Parquet's compression erase these columns (measured at barely 3x savings when it did), and a value shared
     * across rows would let dictionary encoding do the same. A fixed seed keeps the file byte-identical run to run.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Seeded pseudo-random string so the fat sub-columns survive compression and dictionary encoding, keeping the bytes-read comparison meaningful")
    private static String fatValue(int field, int row) {
        java.util.Random random = new java.util.Random(field * 1_000_003L + row);
        StringBuilder value = new StringBuilder(FAT_VALUE_LENGTH);
        for (int c = 0; c < FAT_VALUE_LENGTH; c++) {
            value.append((char) ('a' + random.nextInt(26)));
        }
        return value.toString();
    }

    /**
     * An {@link org.apache.iceberg.io.InputFile} that counts the bytes its streams actually deliver. Both read paths
     * take an Iceberg InputFile, so wrapping here measures the pruned and unpruned reads on identical terms.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Counting InputFile wrapper so the bytes-read comparison measures real I/O instead of footer-derived column sizes")
    private static final class CountingInputFile implements org.apache.iceberg.io.InputFile {
        private final org.apache.iceberg.io.InputFile delegate;
        private final java.util.concurrent.atomic.AtomicLong bytes = new java.util.concurrent.atomic.AtomicLong();

        private CountingInputFile(org.apache.iceberg.io.InputFile delegate) {
            this.delegate = delegate;
        }

        private long bytesRead() {
            return bytes.get();
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public String location() {
            return delegate.location();
        }

        @Override
        public boolean exists() {
            return delegate.exists();
        }

        @Override
        public org.apache.iceberg.io.SeekableInputStream newStream() {
            org.apache.iceberg.io.SeekableInputStream in = delegate.newStream();
            return new org.apache.iceberg.io.SeekableInputStream() {
                @Override
                public long getPos() throws java.io.IOException {
                    return in.getPos();
                }

                @Override
                public void seek(long newPos) throws java.io.IOException {
                    in.seek(newPos);
                }

                @Override
                public int read() throws java.io.IOException {
                    int b = in.read();
                    if (b >= 0) {
                        bytes.incrementAndGet();
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws java.io.IOException {
                    int n = in.read(b, off, len);
                    if (n > 0) {
                        bytes.addAndGet(n);
                    }
                    return n;
                }

                @Override
                public void close() throws java.io.IOException {
                    in.close();
                }
            };
        }
    }

    // =====================================================================================================
    // Part 5 — predicate pushdown on shredded variant sub-fields (manifest bounds -> data-file skipping)
    // =====================================================================================================

    @Test
    public void rewrite_nullsAndNoVariant_areUnchanged() {
        org.apache.iceberg.Schema noVariant = new org.apache.iceberg.Schema(org.apache.iceberg.types.Types.NestedField
                .required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
        Assert.assertNull(VariantPredicateRewriter.rewriteAssumingNesting(null, icebergSchema()));
        org.apache.iceberg.expressions.Expression p = org.apache.iceberg.expressions.Expressions.equal("id", 1);
        Assert.assertSame("no schema -> unchanged", p, VariantPredicateRewriter.rewriteAssumingNesting(p, null));
        Assert.assertSame("no variant column -> unchanged", p,
                VariantPredicateRewriter.rewriteAssumingNesting(p, noVariant));
    }

    @Test
    public void rewrite_topLevelAndStructRefs_areUnchanged() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression flat = org.apache.iceberg.expressions.Expressions.equal("id", 1);
        Assert.assertSame("top-level column untouched", flat,
                VariantPredicateRewriter.rewriteAssumingNesting(flat, schema));
        // a_struct is a real Iceberg struct: Iceberg binds "a_struct.x" itself, so it must not be rewritten.
        org.apache.iceberg.expressions.Expression struct =
                org.apache.iceberg.expressions.Expressions.equal("a_struct.x", "v");
        Assert.assertSame("real struct path untouched", struct,
                VariantPredicateRewriter.rewriteAssumingNesting(struct, schema));
    }

    @Test
    public void rewrite_variantSubPath_becomesExtractTerm() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression rewritten = VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".status", "ACTIVE"), schema);
        // The rewritten predicate must bind against the schema — the original dotted form cannot.
        org.apache.iceberg.expressions.Expression bound =
                org.apache.iceberg.expressions.Binder.bind(schema.asStruct(), rewritten, true);
        Assert.assertNotNull(bound);
        Assert.assertTrue("must use an extract term", rewritten.toString().contains("$.status"));
    }

    @Test
    public void rewrite_dottedVariantPathUnbindableBeforeRewrite() {
        org.apache.iceberg.Schema schema = icebergSchema();
        // Proves why the rewrite is needed at all: the dotted form cannot bind for a variant column.
        try {
            org.apache.iceberg.expressions.Binder.bind(schema.asStruct(),
                    org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".status", "ACTIVE"), true);
            Assert.fail("expected binding of a dotted variant sub-field to fail");
        } catch (RuntimeException expected) {
            // expected: 'variant_field.status' is not a schema field
        }
    }

    @Test
    public void rewrite_nestedPathAndAllComparisons() {
        org.apache.iceberg.Schema schema = icebergSchema();
        Assert.assertTrue(VariantPredicateRewriter
                .rewriteAssumingNesting(org.apache.iceberg.expressions.Expressions.lessThan(COLUMN + ".a.b", 5), schema)
                .toString().contains("$.a.b"));
        // Every ordered comparison is supported.
        Assert.assertNotNull(VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.greaterThanOrEqual(COLUMN + ".n", 1L), schema));
        Assert.assertNotNull(VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.notEqual(COLUMN + ".s", "x"), schema));
    }

    @Test
    public void rewrite_preservesBooleanStructure() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression combined = org.apache.iceberg.expressions.Expressions.and(
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".status", "ACTIVE"),
                org.apache.iceberg.expressions.Expressions.or(org.apache.iceberg.expressions.Expressions.equal("id", 1),
                        org.apache.iceberg.expressions.Expressions
                                .not(org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".n", 7))));
        org.apache.iceberg.expressions.Expression rewritten =
                VariantPredicateRewriter.rewriteAssumingNesting(combined, schema);
        // Both variant refs rewritten, structure intact, and the whole thing now binds.
        String s = rewritten.toString();
        Assert.assertTrue(s.contains("$.status"));
        Assert.assertTrue(s.contains("$.n"));
        Assert.assertNotNull(org.apache.iceberg.expressions.Binder.bind(schema.asStruct(), rewritten, true));
    }

    /**
     * End-to-end proof that a rewritten predicate actually prunes data files: writes a real shredded file, computes
     * real metrics (as the fixtures now do), and runs Iceberg's own {@code InclusiveMetricsEvaluator}. A value inside
     * the sub-field's [min, max] must be kept; one outside must skip the file.
     */
    @Test
    public void endToEnd_variantSubFieldBoundsSkipFiles() throws Exception {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()));
        java.io.File dir = java.nio.file.Files.createTempDirectory("variant-bounds").toFile();
        java.io.File dataFile = new java.io.File(dir, "shredded.parquet");

        org.apache.iceberg.variants.VariantMetadata meta =
                org.apache.iceberg.variants.Variants.metadata("name", "amount");
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.iceberg.variants.ShreddedObject probe = org.apache.iceberg.variants.Variants.object(meta);
        probe.put("name", org.apache.iceberg.variants.Variants.of("m"));
        probe.put("amount", org.apache.iceberg.variants.Variants.of(50));
        Type typedValue =
                (Type) toParquetSchema.invoke(null, org.apache.iceberg.variants.Variant.of(meta, probe).value());

        org.apache.iceberg.data.GenericRecord template = org.apache.iceberg.data.GenericRecord.create(schema);
        String[] names = { "bravo", "delta", "alpha", "echo" };
        int[] amounts = { 30, 40, 20, 50 };
        try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> writer =
                org.apache.iceberg.parquet.Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
                        .variantShreddingFunc((fieldId, n) -> typedValue).build()) {
            for (int i = 0; i < names.length; i++) {
                org.apache.iceberg.variants.ShreddedObject o = org.apache.iceberg.variants.Variants.object(meta);
                o.put("name", org.apache.iceberg.variants.Variants.of(names[i]));
                o.put("amount", org.apache.iceberg.variants.Variants.of(amounts[i]));
                org.apache.iceberg.data.Record row = template.copy();
                row.setField("id", i + 1);
                row.setField(COLUMN, org.apache.iceberg.variants.Variant.of(meta, o));
                writer.add(row);
            }
        }

        org.apache.iceberg.Metrics metrics = org.apache.iceberg.parquet.ParquetUtil.fileMetrics(
                org.apache.iceberg.Files.localInput(dataFile), org.apache.iceberg.MetricsConfig.getDefault());
        Assert.assertNotNull("variant column must have bounds", metrics.lowerBounds());
        Assert.assertTrue("bounds must cover the variant column (field id 2)",
                metrics.lowerBounds().containsKey(2) && metrics.upperBounds().containsKey(2));

        // The bound for a variant column is itself a Variant object keyed by normalized JSON path. Variant buffers
        // must be read little-endian.
        org.apache.iceberg.variants.Variant lower = org.apache.iceberg.variants.Variant
                .from(metrics.lowerBounds().get(2).duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN));
        org.apache.iceberg.variants.Variant upper = org.apache.iceberg.variants.Variant
                .from(metrics.upperBounds().get(2).duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN));
        Assert.assertEquals("alpha", lower.value().asObject().get("$['name']").asPrimitive().get());
        Assert.assertEquals("echo", upper.value().asObject().get("$['name']").asPrimitive().get());

        org.apache.iceberg.DataFile df =
                org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
                        .withPath(dataFile.getAbsolutePath()).withFormat(org.apache.iceberg.FileFormat.PARQUET)
                        .withFileSizeInBytes(dataFile.length()).withMetrics(metrics).build();

        // Predicates as the rewriter produces them (from the dotted form the filter builder emits).
        org.apache.iceberg.expressions.Expression inRange = VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".name", "delta"), schema);
        org.apache.iceberg.expressions.Expression outOfRange = VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".name", "zzzz"), schema);

        Assert.assertTrue("value within [alpha, echo] must not skip the file",
                new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema, inRange).eval(df));
        Assert.assertFalse("value outside [alpha, echo] must skip the file",
                new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema, outOfRange).eval(df));

        // Same for a numeric sub-field (amounts are 20..50).
        Assert.assertTrue(new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema,
                VariantPredicateRewriter.rewriteAssumingNesting(
                        org.apache.iceberg.expressions.Expressions.greaterThan(COLUMN + ".amount", 45), schema))
                                .eval(df));
        Assert.assertFalse(new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema,
                VariantPredicateRewriter.rewriteAssumingNesting(
                        org.apache.iceberg.expressions.Expressions.greaterThan(COLUMN + ".amount", 999), schema))
                                .eval(df));
    }

    // =====================================================================================================
    // Part 6 — the TableScan workaround: extract terms must be stripped from the pushed filter, but only in
    // ways that WEAKEN it (a scan filter may admit more files, never fewer)
    // =====================================================================================================

    /**
     * TODO(iceberg-15384): this is the tripwire — it asserts the <em>defect</em> still exists, so it will start
     * failing the moment we upgrade past https://github.com/apache/iceberg/pull/15384. That failure is the signal to
     * delete the workarounds (see the removal checklist on {@code VariantBoundsEvaluator}) and this test with them.
     */
    @Test
    public void scanWorkaround_icebergCannotSanitizeExtract() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression unbound = org.apache.iceberg.expressions.Expressions
                .equal(org.apache.iceberg.expressions.Expressions.extract(COLUMN, "$.a", "int"), 1);
        for (org.apache.iceberg.expressions.Expression e : new org.apache.iceberg.expressions.Expression[] { unbound,
                org.apache.iceberg.expressions.Binder.bind(schema.asStruct(), unbound, true) }) {
            try {
                org.apache.iceberg.expressions.ExpressionUtil.toSanitizedString(e);
                Assert.fail("expected Iceberg to reject sanitizing an extract term: " + e);
            } catch (UnsupportedOperationException expected) {
                Assert.assertTrue(expected.getMessage().contains("Unsupported term"));
            }
        }
    }

    @Test
    public void scanWorkaround_detectsExtractTerms() {
        org.apache.iceberg.Schema schema = icebergSchema();
        Assert.assertFalse(VariantPredicateRewriter
                .containsExtractTerm(org.apache.iceberg.expressions.Expressions.equal("id", 1)));
        Assert.assertTrue(VariantPredicateRewriter.containsExtractTerm(VariantPredicateRewriter
                .rewriteAssumingNesting(org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 1), schema)));
    }

    /** The stripped filter must contain no extract term, and must be sanitizable (i.e. usable by TableScan). */
    @Test
    public void scanWorkaround_strippedFilterIsScanSafe() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression rewritten =
                VariantPredicateRewriter
                        .rewriteAssumingNesting(
                                org.apache.iceberg.expressions.Expressions.and(
                                        org.apache.iceberg.expressions.Expressions.equal("id", 1),
                                        org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 2)),
                                schema);
        org.apache.iceberg.expressions.Expression stripped = VariantPredicateRewriter.withoutExtractTerms(rewritten);
        Assert.assertFalse(VariantPredicateRewriter.containsExtractTerm(stripped));
        // The whole point: this must not throw the way the un-stripped expression does.
        Assert.assertNotNull(org.apache.iceberg.expressions.ExpressionUtil.toSanitizedString(stripped));
        // The surviving non-variant conjunct is retained, so we do not lose ordinary pushdown.
        Assert.assertTrue(stripped.toString().contains("id"));
    }

    /** AND keeps the other conjunct (weaker = safe). */
    @Test
    public void scanWorkaround_andKeepsNonVariantConjunct() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression stripped = VariantPredicateRewriter
                .withoutExtractTerms(VariantPredicateRewriter.rewriteAssumingNesting(
                        org.apache.iceberg.expressions.Expressions.and(
                                org.apache.iceberg.expressions.Expressions.greaterThan("id", 10),
                                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 2)),
                        schema));
        Assert.assertTrue(stripped.toString().contains("id"));
        Assert.assertFalse(stripped.toString().contains("$.a"));
    }

    /** OR containing an extract term must collapse to alwaysTrue — dropping a disjunct would over-prune. */
    @Test
    public void scanWorkaround_orWithExtractBecomesAlwaysTrue() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression stripped = VariantPredicateRewriter
                .withoutExtractTerms(VariantPredicateRewriter.rewriteAssumingNesting(
                        org.apache.iceberg.expressions.Expressions.or(
                                org.apache.iceberg.expressions.Expressions.equal("id", 1),
                                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 2)),
                        schema));
        Assert.assertEquals(org.apache.iceberg.expressions.Expressions.alwaysTrue(), stripped);
    }

    /** NOT of an extract term must also collapse to alwaysTrue (a weaker child makes a stronger negation). */
    @Test
    public void scanWorkaround_notWithExtractBecomesAlwaysTrue() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression stripped =
                VariantPredicateRewriter
                        .withoutExtractTerms(
                                VariantPredicateRewriter.rewriteAssumingNesting(
                                        org.apache.iceberg.expressions.Expressions.not(
                                                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 2)),
                                        schema));
        Assert.assertEquals(org.apache.iceberg.expressions.Expressions.alwaysTrue(), stripped);
    }

    /**
     * The pruning is not lost by stripping: the full expression still skips files through
     * {@code InclusiveMetricsEvaluator}, which is exactly what the reader factory applies per planned task.
     */
    @Test
    public void scanWorkaround_fullExpressionStillPrunesViaEvaluator() throws Exception {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, COLUMN,
                        org.apache.iceberg.types.Types.VariantType.get()));
        org.apache.iceberg.DataFile df = writeShreddedBucketFile(schema, 20, 50);

        // AND(id > 0, variant.amount > 999999): stripped filter keeps only id, evaluator still prunes on the variant.
        org.apache.iceberg.expressions.Expression rewritten = VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.and(
                        org.apache.iceberg.expressions.Expressions.greaterThan("id", 0),
                        org.apache.iceberg.expressions.Expressions.greaterThan(COLUMN + ".amount", 999999)),
                schema);
        Assert.assertFalse("out-of-range variant predicate must still skip the file",
                new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema, rewritten).eval(df));

        org.apache.iceberg.expressions.Expression inRange = VariantPredicateRewriter.rewriteAssumingNesting(
                org.apache.iceberg.expressions.Expressions.greaterThan(COLUMN + ".amount", 25), schema);
        Assert.assertTrue("in-range variant predicate must keep the file",
                new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(schema, inRange).eval(df));
    }

    /** Writes a one-column shredded variant file whose {@code amount} sub-field spans [min, max]. */
    private static org.apache.iceberg.DataFile writeShreddedBucketFile(org.apache.iceberg.Schema schema, int min,
            int max) throws Exception {
        java.io.File dir = java.nio.file.Files.createTempDirectory("scan-workaround").toFile();
        java.io.File f = new java.io.File(dir, "d.parquet");
        org.apache.iceberg.variants.VariantMetadata meta = org.apache.iceberg.variants.Variants.metadata("amount");
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", org.apache.iceberg.variants.VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.iceberg.variants.ShreddedObject probe = org.apache.iceberg.variants.Variants.object(meta);
        probe.put("amount", org.apache.iceberg.variants.Variants.of(min));
        Type typedValue =
                (Type) toParquetSchema.invoke(null, org.apache.iceberg.variants.Variant.of(meta, probe).value());
        org.apache.iceberg.data.GenericRecord template = org.apache.iceberg.data.GenericRecord.create(schema);
        try (org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.Record> w =
                org.apache.iceberg.parquet.Parquet.write(org.apache.iceberg.Files.localOutput(f)).schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create)
                        .variantShreddingFunc((fieldId, n) -> typedValue).build()) {
            for (int amt : new int[] { min, max }) {
                org.apache.iceberg.variants.ShreddedObject o = org.apache.iceberg.variants.Variants.object(meta);
                o.put("amount", org.apache.iceberg.variants.Variants.of(amt));
                org.apache.iceberg.data.Record r = template.copy();
                r.setField("id", amt);
                r.setField(COLUMN, org.apache.iceberg.variants.Variant.of(meta, o));
                w.add(r);
            }
        }
        org.apache.iceberg.Metrics metrics = org.apache.iceberg.parquet.ParquetUtil
                .fileMetrics(org.apache.iceberg.Files.localInput(f), org.apache.iceberg.MetricsConfig.getDefault());
        return org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
                .withPath(f.getAbsolutePath()).withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withFileSizeInBytes(f.length()).withMetrics(metrics).build();
    }

    // =====================================================================================================
    // Part 7 — the pushed filter must always be BINDABLE. A dotted reference into a variant cannot bind, so any
    // predicate the rewriter declines has to be stripped rather than pushed, or scan planning fails outright.
    // =====================================================================================================

    /**
     * The property every other test in this part depends on: Iceberg refuses to bind a dotted variant reference. If
     * this ever starts passing, the stripping below becomes unnecessary — and this test is how you find out.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Tripwire for the premise: asserts Iceberg still refuses to bind a dotted variant reference")
    @Test
    public void bindability_dottedVariantReferenceCannotBind() {
        org.apache.iceberg.Schema schema = icebergSchema();
        try {
            org.apache.iceberg.expressions.Binder.bind(schema.asStruct(),
                    org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 1), true);
            Assert.fail("expected Iceberg to reject binding a dotted variant reference");
        } catch (org.apache.iceberg.exceptions.ValidationException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("Cannot find field"));
        }
    }

    /**
     * Every shape {@link VariantPredicateRewriter#rewrite} declines, run through the full pipeline the reader factory
     * uses, asserting the result binds. Each of these previously reached {@code TableScan.filter(..)} as a dotted
     * reference and would have failed the query rather than skipping the optimization.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Runs every declined-rewrite shape through the reader factory's filter pipeline and asserts the result binds; these previously reached TableScan as unbindable dotted references")
    @Test
    public void bindability_declinedRewritesAreStrippedNotPushed() {
        org.apache.iceberg.Schema schema = icebergSchema();
        java.util.Map<String, org.apache.iceberg.expressions.Expression> declined = new java.util.LinkedHashMap<>();
        // No literal to type the extract with.
        declined.put("isNull", org.apache.iceberg.expressions.Expressions.isNull(COLUMN + ".a"));
        declined.put("notNull", org.apache.iceberg.expressions.Expressions.notNull(COLUMN + ".a"));
        // Literal types extractTypeFor does not support: temporals arrive as these and would compare against the
        // wrong bound type, so the rewrite refuses them by design.
        declined.put("bigDecimal",
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", new java.math.BigDecimal("1.5")));
        declined.put("byteBuffer", org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a",
                java.nio.ByteBuffer.wrap(new byte[] { 1, 2 })));
        declined.put("uuid", org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a",
                java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000")));
        // A segment dot notation cannot express unambiguously.
        declined.put("bracketPath", org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a[0]", 1));

        for (java.util.Map.Entry<String, org.apache.iceberg.expressions.Expression> entry : declined.entrySet()) {
            org.apache.iceberg.expressions.Expression pushed = pushedFilter(entry.getValue(), schema);
            Assert.assertFalse(entry.getKey() + ": a declined rewrite must not survive into the pushed filter",
                    VariantPredicateRewriter.containsVariantSubFieldPredicate(pushed, schema));
            assertBinds(entry.getKey(), pushed, schema);
        }
    }

    /** A rewrite that succeeds must still leave a bindable filter once the extract terms are split out. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Confirms a successful rewrite also leaves a bindable pushed filter after the extract split")
    @Test
    public void bindability_successfulRewriteIsAlsoBindable() {
        org.apache.iceberg.Schema schema = icebergSchema();
        assertBinds("rewritten",
                pushedFilter(org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 1), schema), schema);
    }

    /** Stripping must weaken, never strengthen: under OR and NOT the whole node collapses to alwaysTrue. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Asserts variant sub-field stripping weakens under OR/NOT and preserves ordinary conjuncts under AND")
    @Test
    public void stripping_variantSubFieldWeakensUnderOrAndNot() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression variantPredicate =
                org.apache.iceberg.expressions.Expressions.isNull(COLUMN + ".a");
        org.apache.iceberg.expressions.Expression idPredicate =
                org.apache.iceberg.expressions.Expressions.equal("id", 1);

        // OR: dropping just the variant branch would EXCLUDE files matching it — the whole OR must go.
        Assert.assertEquals(org.apache.iceberg.expressions.Expressions.alwaysTrue(),
                VariantPredicateRewriter.withoutVariantSubFieldPredicates(
                        org.apache.iceberg.expressions.Expressions.or(variantPredicate, idPredicate), schema));
        // NOT: replacing the child with alwaysTrue would make the NOT alwaysFalse — stronger, and wrong.
        Assert.assertEquals(org.apache.iceberg.expressions.Expressions.alwaysTrue(),
                VariantPredicateRewriter.withoutVariantSubFieldPredicates(
                        org.apache.iceberg.expressions.Expressions.not(variantPredicate), schema));
        // AND: the ordinary conjunct survives, so pruning on real columns is not lost.
        org.apache.iceberg.expressions.Expression and = VariantPredicateRewriter.withoutVariantSubFieldPredicates(
                org.apache.iceberg.expressions.Expressions.and(variantPredicate, idPredicate), schema);
        Assert.assertFalse(VariantPredicateRewriter.containsVariantSubFieldPredicate(and, schema));
        Assert.assertTrue("the ordinary-column predicate must be kept", and.toString().contains("id"));
        assertBinds("and", and, schema);
    }

    /** A dotted reference into a real struct is Iceberg's business and must be left alone. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Guards against over-stripping: dotted references into a real struct must be untouched")
    @Test
    public void stripping_leavesRealStructReferencesAlone() {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "st",
                        org.apache.iceberg.types.Types.StructType.of(org.apache.iceberg.types.Types.NestedField
                                .optional(3, "a", org.apache.iceberg.types.Types.IntegerType.get()))));
        org.apache.iceberg.expressions.Expression structPredicate =
                org.apache.iceberg.expressions.Expressions.equal("st.a", 1);
        Assert.assertSame(structPredicate,
                VariantPredicateRewriter.withoutVariantSubFieldPredicates(structPredicate, schema));
        assertBinds("struct", structPredicate, schema);
    }

    /**
     * The flag-off path: no rewrite at all, just stripping. It must still yield a bindable filter that keeps the
     * ordinary-column predicates, which is what "fall back to how it behaved before this feature" means.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Covers the variantStatsPushdown=false path, which strips without rewriting at all")
    @Test
    public void flagOff_strippingAloneLeavesABindableFilter() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression original = org.apache.iceberg.expressions.Expressions.and(
                org.apache.iceberg.expressions.Expressions.equal(COLUMN + ".a", 1),
                org.apache.iceberg.expressions.Expressions.greaterThan("id", 10));
        // No rewrite() call — exactly what configure() does when variantStatsPushdown is false.
        org.apache.iceberg.expressions.Expression pushed =
                VariantPredicateRewriter.withoutVariantSubFieldPredicates(original, schema);
        Assert.assertFalse(VariantPredicateRewriter.containsVariantSubFieldPredicate(pushed, schema));
        Assert.assertFalse("no extract term is produced when the rewrite is skipped",
                VariantPredicateRewriter.containsExtractTerm(pushed));
        Assert.assertTrue("the ordinary-column predicate must survive", pushed.toString().contains("id"));
        assertBinds("flagOff", pushed, schema);
    }

    /**
     * A variant nested inside a struct. Iceberg cannot resolve a name through a variant at any depth — both
     * {@code st.v} and {@code st.v.x} fail to bind — so these must be stripped like top-level ones. Catching them
     * here rather than in the reader factory's fail-safe matters: the fail-safe drops the entire filter, which would
     * also lose pushdown on the ordinary columns ANDed with it.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Covers variants nested inside structs, which the first version of the stripper missed because it only inspected the root column")
    @Test
    public void stripping_handlesVariantNestedInsideAStruct() {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "st",
                        org.apache.iceberg.types.Types.StructType.of(org.apache.iceberg.types.Types.NestedField
                                .optional(3, "v", org.apache.iceberg.types.Types.VariantType.get()))));

        for (String reference : new String[] { "st.v", "st.v.x" }) {
            org.apache.iceberg.expressions.Expression pushed =
                    VariantPredicateRewriter
                            .withoutVariantSubFieldPredicates(
                                    org.apache.iceberg.expressions.Expressions.and(
                                            org.apache.iceberg.expressions.Expressions.equal(reference, 1),
                                            org.apache.iceberg.expressions.Expressions.greaterThan("id", 10)),
                                    schema);
            assertBinds(reference, pushed, schema);
            Assert.assertTrue(reference + ": the ordinary-column conjunct must survive rather than the whole filter "
                    + "being discarded", pushed.toString().contains("id"));
        }
    }

    /**
     * The one shape stripping cannot classify: a reference to a column that is not in the schema at all. No prefix of
     * it resolves, so it is not recognised as a variant sub-field and survives into the pushed filter — where it is
     * still unbindable. This documents that the reader factory's {@code bindableOrNoFilter} backstop is what covers
     * it: the filter is discarded, the scan reads more files, and the query succeeds. Completes the enumeration of
     * unbindable shapes.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Documents the only unbindable shape stripping cannot classify (unknown column) and that the factory bind-rehearsal backstop covers it")
    @Test
    public void bindability_unknownColumnIsNotStrippedAndNeedsTheBackstop() {
        org.apache.iceberg.Schema schema = icebergSchema();
        org.apache.iceberg.expressions.Expression unknown =
                org.apache.iceberg.expressions.Expressions.equal("nosuchcolumn", 1);
        // Stripping leaves it alone — it is not a variant reference.
        Assert.assertFalse(VariantPredicateRewriter.containsVariantSubFieldPredicate(unknown, schema));
        Assert.assertSame(unknown, VariantPredicateRewriter.withoutVariantSubFieldPredicates(unknown, schema));
        // And it does not bind, which is precisely why the factory rehearses the bind before pushing.
        try {
            org.apache.iceberg.expressions.Binder.bind(schema.asStruct(), unknown, true);
            Assert.fail("expected an unknown column to be unbindable");
        } catch (org.apache.iceberg.exceptions.ValidationException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("Cannot find field"));
        }
    }

    /** Mirrors the reader factory: rewrite, split out extract terms, then strip anything still unbindable. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Mirrors IcebergParquetRecordReaderFactory's filter pipeline so the tests exercise what production pushes")
    private static org.apache.iceberg.expressions.Expression pushedFilter(
            org.apache.iceberg.expressions.Expression original, org.apache.iceberg.Schema schema) {
        org.apache.iceberg.expressions.Expression rewritten =
                VariantPredicateRewriter.rewriteAssumingNesting(original, schema);
        if (VariantPredicateRewriter.containsExtractTerm(rewritten)) {
            rewritten = VariantPredicateRewriter.withoutExtractTerms(rewritten);
        }
        return VariantPredicateRewriter.withoutVariantSubFieldPredicates(rewritten, schema);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Fails with the binding error and the offending expression rather than a bare assertion failure")
    private static void assertBinds(String label, org.apache.iceberg.expressions.Expression expression,
            org.apache.iceberg.Schema schema) {
        try {
            org.apache.iceberg.expressions.Binder.bind(schema.asStruct(), expression, true);
        } catch (RuntimeException e) {
            Assert.fail(label + ": pushed filter must bind, but got " + e.getClass().getSimpleName() + ": "
                    + e.getMessage() + " for " + expression);
        }
    }

    // =====================================================================================================
    // helpers
    // =====================================================================================================

    private static Type bin(String name) {
        return Types.optional(PrimitiveTypeName.BINARY).named(name);
    }

    /** A shredded scalar field: group name { value: binary; typed_value: binary }. */
    private static GroupType scalar(String name) {
        return Types.buildGroup(Repetition.OPTIONAL).addField(bin(VALUE)).addField(bin(TYPED_VALUE)).named(name);
    }

    /** A shredded object field: group name { value: binary; typed_value: group { members... } }. */
    private static GroupType object(String name, Type... members) {
        GroupType typed = Types.buildGroup(Repetition.OPTIONAL).addFields(members).named(TYPED_VALUE);
        return Types.buildGroup(Repetition.OPTIONAL).addField(bin(VALUE)).addField(typed).named(name);
    }

    /** The object typed_value group of a variant (its fields are the object's members). */
    private static GroupType objectTyped(Type... members) {
        return Types.buildGroup(Repetition.OPTIONAL).addFields(members).named(TYPED_VALUE);
    }

    private static GroupType variant(GroupType typedValue) {
        return variantNamed(COLUMN, typedValue);
    }

    private static GroupType variantNamed(String name, GroupType typedValue) {
        return Types.buildGroup(Repetition.OPTIONAL).addField(bin(METADATA)).addField(bin(VALUE)).addField(typedValue)
                .named(name);
    }

    private static MessageType message(GroupType variant) {
        return new MessageType("table", List.of(Types.optional(PrimitiveTypeName.INT32).named("id"), variant));
    }

    private static MessageType clip(MessageType schema, RequestedVariantPaths paths) {
        return VariantSchemaClipper.clip(schema, COLUMN, paths);
    }

    private static GroupType clippedVariant(MessageType schema, RequestedVariantPaths paths) {
        return clip(schema, paths).getType(COLUMN).asGroupType();
    }

    private static Set<String> typedValueFieldNames(GroupType variant) {
        return asSet(fieldNames(variant.getType(TYPED_VALUE).asGroupType()));
    }

    private static List<String> fieldNames(GroupType group) {
        List<String> names = new ArrayList<>();
        group.getFields().forEach(f -> names.add(f.getName()));
        return names;
    }

    private static Set<String> asSet(java.util.Collection<String> c) {
        return new TreeSet<>(c);
    }

    private static ARecordType projected(List<List<String>> paths) throws Exception {
        return ProjectionFiltrationTypeUtil.getRecordType(paths);
    }

    private static RequestedVariantPaths pathsFor(List<List<String>> queryPaths) throws Exception {
        return RequestedVariantPaths.fromProjectedType(projected(queryPaths), COLUMN);
    }
}
