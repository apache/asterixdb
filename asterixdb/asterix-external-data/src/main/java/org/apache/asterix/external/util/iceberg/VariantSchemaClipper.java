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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Variant sub-path projection pushdown: clips the Parquet schema of a shredded {@code VARIANT} column so its
 * {@code typed_value} group keeps only the shredded sub-columns a query actually requests (per {@link
 * RequestedVariantPaths}). It is a pure schema-in/schema-out transform — no I/O, no Iceberg dependency — so it can be
 * unit-tested in isolation. The clipped schema is used as both the physical Parquet requested schema (to prune reads)
 * and the input to Iceberg's variant reader builder.
 * <p>
 * A shredded variant column in Parquet is a recursive group:
 *
 * <pre>
 * variant_field: group {
 *   metadata:    binary                     // ALWAYS kept
 *   value:       binary                     // residual remainder — ALWAYS kept
 *   typed_value: group {                    // the shredded sub-columns
 *     &lt;field&gt;: group { value: binary; typed_value: &lt;scalar | object group | array group&gt; }
 *     ...
 *   }
 * }
 * </pre>
 *
 * The clipper keeps {@code metadata} and {@code value} unconditionally (the residual is required to reconstruct
 * anything not shredded, and any requested-but-residual field), and rebuilds {@code typed_value} to retain only the
 * requested member fields, recursing into nested object members.
 * <p>
 * Correctness is preserved even when a variant is only partially clipped: keeping {@code value} means unrequested
 * residual fields may still reconstruct (harmlessly — downstream projection drops them), while dropped
 * {@code typed_value} members are exactly the shredded sub-columns the query never references. Every uncertain case
 * (serialized column, array shredding, scalar {@code typed_value}, a group that cannot be reproduced, or "keep all")
 * falls back to returning the input unchanged, so the transform can only ever remove provably-unreferenced sub-columns.
 * Rebuilt groups keep their repetition, name, field id and logical-type annotation (a variant group is annotated, e.g.
 * {@code VARIANT(1)}), so the clipped schema still resolves against the file's columns.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Variant projection pushdown: prunes a Parquet variant typed_value group to the requested "
        + "sub-fields; keeps metadata+value always; safe no-op on serialized/array/scalar/annotated groups")
public final class VariantSchemaClipper {

    // Iceberg's variant shredding field names (org.apache.iceberg.parquet.ParquetVariantVisitor).
    static final String METADATA = "metadata";
    static final String VALUE = "value";
    static final String TYPED_VALUE = "typed_value";
    static final String LIST = "list";

    private VariantSchemaClipper() {
    }

    /**
     * Returns a copy of {@code fileSchema} in which the top-level variant column {@code columnName} is clipped to
     * {@code paths}. Returns {@code fileSchema} unchanged when the column is absent, is not a group, or nothing can be
     * pruned.
     */
    public static MessageType clip(MessageType fileSchema, String columnName, RequestedVariantPaths paths) {
        if (fileSchema == null || paths == null || paths.isAll() || !fileSchema.containsField(columnName)) {
            return fileSchema;
        }
        Type column = fileSchema.getType(columnName);
        if (column.isPrimitive()) {
            return fileSchema;
        }
        GroupType clippedColumn = clipValueGroup(column.asGroupType(), paths);
        if (clippedColumn == column) {
            return fileSchema;
        }
        List<Type> newFields = new ArrayList<>(fileSchema.getFieldCount());
        for (Type field : fileSchema.getFields()) {
            newFields.add(field.getName().equals(columnName) ? clippedColumn : field);
        }
        return new MessageType(fileSchema.getName(), newFields);
    }

    /**
     * Clips a "value group" — a variant group ({@code metadata}, {@code value}, {@code typed_value}) or a nested
     * shredded-field group ({@code value}, {@code typed_value}) — to {@code paths}. Non-{@code typed_value} fields
     * (i.e. {@code metadata}/{@code value}) are always retained. Returns the same instance when nothing changed.
     */
    private static GroupType clipValueGroup(GroupType group, RequestedVariantPaths paths) {
        if (paths.isAll() || !group.containsField(TYPED_VALUE)) {
            return group;
        }
        Type typedValue = group.getType(TYPED_VALUE);
        // Only an object typed_value (a group whose fields are the object's members) can be sub-selected. A scalar
        // (primitive) or an array (a group holding the repeated 'list' element) is kept whole for now.
        if (typedValue.isPrimitive()) {
            return group;
        }
        GroupType typedValueGroup = typedValue.asGroupType();
        if (typedValueGroup.containsField(LIST)) {
            return group; // array shredding: element projection not supported yet
        }

        List<Type> keptMembers = new ArrayList<>();
        boolean changed = false;
        for (Type member : typedValueGroup.getFields()) {
            RequestedVariantPaths childPaths = paths.child(member.getName());
            if (childPaths == null) {
                changed = true; // member not requested -> drop
                continue;
            }
            if (childPaths.isAll() || member.isPrimitive()) {
                keptMembers.add(member); // keep the whole member
                continue;
            }
            GroupType clippedMember = clipValueGroup(member.asGroupType(), childPaths);
            changed |= clippedMember != member;
            keptMembers.add(clippedMember);
        }

        if (!changed) {
            return group;
        }

        List<Type> newFields = new ArrayList<>(group.getFieldCount());
        if (keptMembers.isEmpty()) {
            // Every requested field is residual-only here: drop typed_value entirely, keep metadata + value. Only safe
            // if that leaves a non-empty group (Parquet forbids empty groups); otherwise keep the group whole.
            for (Type field : group.getFields()) {
                if (!field.getName().equals(TYPED_VALUE)) {
                    newFields.add(field);
                }
            }
            if (newFields.isEmpty()) {
                return group;
            }
        } else {
            GroupType newTypedValue = rebuild(typedValueGroup, keptMembers);
            if (newTypedValue == null) {
                return group; // could not rebuild faithfully -> keep whole (safe)
            }
            for (Type field : group.getFields()) {
                newFields.add(field.getName().equals(TYPED_VALUE) ? newTypedValue : field);
            }
        }
        GroupType rebuilt = rebuild(group, newFields);
        return rebuilt == null ? group : rebuilt;
    }

    /**
     * Rebuilds {@code original} with {@code fields}, preserving repetition, name, field id and logical-type annotation.
     * Preserving the annotation is essential: a variant column's group is annotated (e.g. {@code VARIANT(1)}), and both
     * Iceberg's reader building and Parquet's column resolution rely on it. Returns {@code null} if the group cannot be
     * reproduced for any reason, in which case the caller keeps the original group unchanged.
     */
    private static GroupType rebuild(GroupType original, List<Type> fields) {
        try {
            Types.GroupBuilder<GroupType> builder = Types.buildGroup(original.getRepetition());
            LogicalTypeAnnotation annotation = original.getLogicalTypeAnnotation();
            if (annotation != null) {
                builder.as(annotation);
            }
            Type.ID id = original.getId();
            if (id != null) {
                builder.id(id.intValue());
            }
            for (Type field : fields) {
                builder.addField(field);
            }
            return builder.named(original.getName());
        } catch (RuntimeException e) {
            return null;
        }
    }
}
