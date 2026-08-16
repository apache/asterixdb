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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Variant sub-path projection pushdown: the per-scan plan of which shredded {@code VARIANT} columns to prune
 * and to which sub-paths. It joins the two sources of truth:
 * <ul>
 * <li>the <b>Iceberg schema</b> — the only place that says a column is a {@code VARIANT} (a real struct/object is not
 * routed here; Iceberg's own column pruning already handles those); and</li>
 * <li>the <b>projected type</b> — the optimizer's requested-fields record, turned into a {@link RequestedVariantPaths}
 * tree per variant column.</li>
 * </ul>
 * A column is entered into the plan only when: pushdown is enabled, the column's Iceberg type is {@code VARIANT}, and
 * the request actually narrows it (not {@link RequestedVariantPaths#isAll() all}). A column requested whole is left off
 * the plan so it stays on the normal, un-clipped read path. When the plan {@link #isEmpty() is empty} the reader does
 * exactly what it does today. This class holds no Parquet types; the read path combines it with the per-file Parquet
 * schema (via {@link VariantSchemaClipper}) to actually prune reads.
 * <p>
 * Variants are found at <b>any struct depth</b>, not just as top-level columns, and each is keyed by the unjoined path
 * that reaches it — {@code ["variant_field"]}, or {@code ["st", "v"]} inside a struct. Segments rather than a dotted
 * name because the join is lossy: a field literally named {@code "st.v"} would collide with the nested reading, and
 * both shapes exist in the fixtures. The walk follows struct nesting only; a variant inside a list or map is left off
 * the plan, matching the predicate side, because element bounds and element clipping are not modelled.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Variant projection gating: builds the map of VARIANT column -> requested sub-paths, gated on the flag, the "
        + "Iceberg VARIANT type, and whether the request actually narrows the column")
public final class VariantProjectionPlan {

    private static final VariantProjectionPlan EMPTY = new VariantProjectionPlan(Collections.emptyMap());

    /** Variant column name -> requested sub-paths; only columns that are variants AND actually narrowed. */
    /** Keyed by the variant's unjoined path: ["variant_field"], or ["st", "v"] when nested in a struct. */
    private final Map<List<String>, RequestedVariantPaths> byColumn;

    private VariantProjectionPlan(Map<List<String>, RequestedVariantPaths> byColumn) {
        this.byColumn = byColumn;
    }

    /** The empty plan: nothing to prune, reader stays on its normal path. */
    public static VariantProjectionPlan none() {
        return EMPTY;
    }

    /**
     * Builds the plan from the projected Iceberg schema and the optimizer's projected type.
     *
     * @param projectedSchema the Iceberg schema of the scan's projected columns (source of truth for VARIANT-ness)
     * @param projectedType   the optimizer's requested-fields record type, or {@code null}
     * @param enabled         whether variant projection pushdown is enabled (the WITH-clause / session flag)
     * @return the plan; {@link #none()} when disabled, there is no schema, or no variant column is narrowed
     */
    public static VariantProjectionPlan from(Schema projectedSchema, ARecordType projectedType, boolean enabled) {
        if (!enabled || projectedSchema == null) {
            return EMPTY;
        }
        Map<List<String>, RequestedVariantPaths> byColumn = new LinkedHashMap<>();
        collectVariants(projectedSchema.asStruct(), new ArrayDeque<>(), projectedType, byColumn);
        return byColumn.isEmpty() ? EMPTY : new VariantProjectionPlan(Collections.unmodifiableMap(byColumn));
    }

    /**
     * Collects every variant reachable through STRUCT nesting, with the path that reaches it.
     * <p>
     * Struct nesting only, deliberately: a variant inside a list or map is left out because the clipper cannot express
     * one, and because the predicate side excludes them too — keeping the two halves of the feature on the same set of
     * shapes. Ordinary structs and primitives are also skipped here; Iceberg's own column projection already prunes
     * those.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Walks the schema for variants at any struct depth, keyed by unjoined path, so a variant nested in a struct gets column pruning like a top-level one")
    private static void collectVariants(Types.StructType struct, Deque<String> path, ARecordType projectedType,
            Map<List<String>, RequestedVariantPaths> byColumn) {
        for (NestedField field : struct.fields()) {
            path.addLast(field.name());
            if (field.type().typeId() == Type.TypeID.VARIANT) {
                List<String> columnPath = List.copyOf(path);
                RequestedVariantPaths paths = RequestedVariantPaths.fromProjectedType(projectedType, columnPath);
                if (!paths.isAll()) {
                    byColumn.put(columnPath, paths); // whole variant requested -> nothing to prune, so skip it
                }
            } else if (field.type().isStructType()) {
                collectVariants(field.type().asStructType(), path, projectedType, byColumn);
            }
            path.removeLast();
        }
    }

    /** @return {@code true} when there is nothing to prune (reader behaves exactly as it does today). */
    public boolean isEmpty() {
        return byColumn.isEmpty();
    }

    /** @return the requested sub-paths for a variant column, or {@code null} if that column is not in the plan. */
    public RequestedVariantPaths get(List<String> columnPath) {
        return byColumn.get(columnPath);
    }

    /** @return the variant column names covered by this plan. */
    public Set<List<String>> columns() {
        return byColumn.keySet();
    }

    @Override
    public String toString() {
        return byColumn.toString();
    }
}
