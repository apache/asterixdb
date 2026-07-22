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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * An immutable tree describing which sub-paths of a single Iceberg {@code VARIANT} column a query actually accesses,
 * derived from the projected {@link ARecordType} the optimizer already computes and serializes into the scan
 * configuration (see {@code ExternalDataConstants#KEY_REQUESTED_FIELDS}).
 * <p>
 * This captures <em>what</em> to keep. {@link VariantSchemaClipper} uses it to clip a Parquet variant
 * {@code typed_value} group so only the requested shredded sub-columns are read. This class is intentionally free of
 * any Iceberg/Parquet dependency so it can be unit-tested in isolation.
 * <p>
 * Two shapes of node exist:
 * <ul>
 * <li><b>{@code all}</b> — the whole value at this position is needed; nothing below may be pruned. This corresponds to
 * a projected-type leaf ({@code ANY}, a scalar, an array, a union, or an empty record), and to a query that selects the
 * bare variant column (e.g. {@code SELECT v.variant_field}) or the whole record (e.g. {@code SELECT *}).</li>
 * <li><b>partial</b> — only the named {@link #requestedFieldNames() child fields} are needed, each described by its own
 * nested {@code RequestedVariantPaths}. This corresponds to a projected-type nested {@link ARecordType} with fields
 * (e.g. {@code SELECT v.variant_field.status}).</li>
 * </ul>
 * The safe default is always {@code all}: anything the projected type does not clearly narrow yields {@code all}, so a
 * consumer never prunes a value the query might need. Pruning is thus best-effort and can only ever remove sub-columns
 * the query provably does not reference.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Variant projection pushdown: extracts the requested variant sub-path tree from the "
        + "optimizer's projected ARecordType; leaf/ANY/non-record => whole value (no pruning)")
public final class RequestedVariantPaths {

    /** Whole value requested; do not prune anything below this node. */
    private static final RequestedVariantPaths ALL = new RequestedVariantPaths(null);

    /** {@code null} iff this node is {@link #ALL}; otherwise an ordered, unmodifiable map of requested child paths. */
    private final Map<String, RequestedVariantPaths> children;

    private RequestedVariantPaths(Map<String, RequestedVariantPaths> children) {
        this.children = children;
    }

    /** The node meaning "keep the whole value" (no pruning below this point). */
    public static RequestedVariantPaths all() {
        return ALL;
    }

    /**
     * Extracts the requested sub-paths of {@code columnName} from a projected record type.
     *
     * @param projectedType the projected type the optimizer produced for the scan, or {@code null}
     * @param columnName    the top-level variant column name (e.g. {@code "variant_field"})
     * @return the requested sub-path tree; {@link #all()} when the whole variant is needed, the column is absent from
     *         the projection, or the projection requests all fields
     */
    public static RequestedVariantPaths fromProjectedType(ARecordType projectedType, String columnName) {
        if (projectedType == null || projectedType == ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE) {
            // No projection, or "all fields" requested -> the whole variant is needed.
            return ALL;
        }
        IAType columnType = projectedType.getFieldType(columnName);
        if (columnType == null) {
            // The column is not individually projected. It is either not read at all, or read whole; either way the
            // safe answer is "keep everything".
            return ALL;
        }
        return build(columnType);
    }

    /** @return {@code true} iff the whole value is requested and nothing below may be pruned. */
    public boolean isAll() {
        return children == null;
    }

    /**
     * @param fieldName a child field name
     * @return the requested sub-paths for {@code fieldName}: {@link #all()} when this node is {@code all}; otherwise the
     *         child node, or {@code null} when {@code fieldName} is not requested
     */
    public RequestedVariantPaths child(String fieldName) {
        return children == null ? ALL : children.get(fieldName);
    }

    /**
     * @return the requested child field names; the empty set when this node is {@code all} (callers should gate on
     *         {@link #isAll()} first, since "all" means "keep every field", not "keep none")
     */
    public Set<String> requestedFieldNames() {
        return children == null ? Collections.emptySet() : children.keySet();
    }

    /**
     * Builds the tree for a projected field's type. A projected leaf is {@code BuiltinType.ANY}; a projected object is a
     * nested {@link ARecordType} with fields. Anything that is not a non-empty record (a scalar, {@code ANY}, an array,
     * a union, or an empty record) means the whole value is needed, so it maps to {@link #ALL}.
     */
    private static RequestedVariantPaths build(IAType type) {
        IAType actual = unwrap(type);
        if (actual.getTypeTag() != ATypeTag.OBJECT || !(actual instanceof ARecordType)) {
            return ALL;
        }
        ARecordType record = (ARecordType) actual;
        String[] fieldNames = record.getFieldNames();
        if (fieldNames.length == 0) {
            // An empty record carries no narrowing information; keep the whole value.
            return ALL;
        }
        IAType[] fieldTypes = record.getFieldTypes();
        Map<String, RequestedVariantPaths> children = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            children.put(fieldNames[i], build(fieldTypes[i]));
        }
        return new RequestedVariantPaths(Collections.unmodifiableMap(children));
    }

    /** Unwraps the projection util's rename wrapper so the underlying record type is inspected. */
    private static IAType unwrap(IAType type) {
        IAType current = type;
        while (current instanceof ProjectionFiltrationTypeUtil.RenamedType) {
            current = current.getType();
        }
        return current;
    }

    @Override
    public String toString() {
        return children == null ? "*" : children.toString();
    }
}
