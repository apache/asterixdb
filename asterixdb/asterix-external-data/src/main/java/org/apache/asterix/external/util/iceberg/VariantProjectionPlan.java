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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
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
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Variant projection gating: builds the map of VARIANT column -> requested sub-paths, gated on the flag, the "
        + "Iceberg VARIANT type, and whether the request actually narrows the column")
public final class VariantProjectionPlan {

    private static final VariantProjectionPlan EMPTY = new VariantProjectionPlan(Collections.emptyMap());

    /** Variant column name -> requested sub-paths; only columns that are variants AND actually narrowed. */
    private final Map<String, RequestedVariantPaths> byColumn;

    private VariantProjectionPlan(Map<String, RequestedVariantPaths> byColumn) {
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
        Map<String, RequestedVariantPaths> byColumn = null;
        for (NestedField field : projectedSchema.columns()) {
            if (field.type().typeId() != Type.TypeID.VARIANT) {
                continue; // structs/primitives are pruned by Iceberg's normal column projection, not here
            }
            RequestedVariantPaths paths = RequestedVariantPaths.fromProjectedType(projectedType, field.name());
            if (paths.isAll()) {
                continue; // whole variant requested -> nothing to prune, leave on the normal read path
            }
            if (byColumn == null) {
                byColumn = new LinkedHashMap<>();
            }
            byColumn.put(field.name(), paths);
        }
        return byColumn == null ? EMPTY : new VariantProjectionPlan(Collections.unmodifiableMap(byColumn));
    }

    /** @return {@code true} when there is nothing to prune (reader behaves exactly as it does today). */
    public boolean isEmpty() {
        return byColumn.isEmpty();
    }

    /** @return the requested sub-paths for a variant column, or {@code null} if that column is not in the plan. */
    public RequestedVariantPaths get(String columnName) {
        return byColumn.get(columnName);
    }

    /** @return the variant column names covered by this plan. */
    public Set<String> columns() {
        return byColumn.keySet();
    }

    @Override
    public String toString() {
        return byColumn.toString();
    }
}
