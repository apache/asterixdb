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
package org.apache.asterix.spidersilk.schema;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;

/**
 * Converts AsterixDB ADM type objects ({@link IAType}) into human-readable strings
 * suitable for inclusion in an LLM prompt.
 *
 * <p>Type rendering rules:
 * <ul>
 *   <li>Primitive types (bigint, string, boolean, …) → lower-cased type name</li>
 *   <li>{@code ARecordType} (nested object) → {@code {field1: type1, field2: type2}}</li>
 *   <li>{@code AOrderedListType} (ordered array) → {@code [itemType]}</li>
 *   <li>{@code AUnorderedListType} (bag/multiset) → {@code {{itemType}}}</li>
 *   <li>{@code AUnionType} (nullable/missable field) → {@code actualType?}</li>
 * </ul>
 *
 * <p>Recursive formatting is limited to {@value #MAX_DEPTH} levels to prevent
 * runaway output for deeply nested types.
 */
public class DatasetSchemaFormatter {

    private static final int MAX_DEPTH = 4;

    /**
     * Formats {@code type} as a human-readable string.
     *
     * @param type the ADM type to format; {@code null} is rendered as {@code "any"}
     * @return a compact, prompt-friendly type description
     */
    public String formatType(IAType type) {
        return formatType(type, 0);
    }

    private String formatType(IAType type, int depth) {
        if (type == null) {
            return "any";
        }
        if (depth >= MAX_DEPTH) {
            return "object";
        }
        switch (type.getTypeTag()) {
            case UNION:
                // Nullable or missable field: unwrap to the actual type and append '?'
                return formatType(((AUnionType) type).getActualType(), depth) + "?";
            case OBJECT:
                return formatRecord((ARecordType) type, depth);
            case ARRAY:
                // Ordered list (SQL++ array syntax: [itemType])
                return "[" + formatType(((AOrderedListType) type).getItemType(), depth + 1) + "]";
            case MULTISET:
                // Unordered list / bag (SQL++ multiset syntax: {{itemType}})
                return "{{" + formatType(((AUnorderedListType) type).getItemType(), depth + 1) + "}}";
            default:
                return type.getTypeName().toLowerCase();
        }
    }

    /**
     * Formats a record type as {@code {field1: type1, field2: type2}}.
     * For top-level fields of a Dataset (depth 0), the outer braces are omitted
     * because the field list is already wrapped by the Dataset description.
     */
    private String formatRecord(ARecordType recordType, int depth) {
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        if (fieldNames.length == 0) {
            return "object";
        }
        StringBuilder sb = new StringBuilder();
        boolean wrapWithBraces = depth > 0;
        if (wrapWithBraces) {
            sb.append('{');
        }
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldNames[i]).append(": ").append(formatType(fieldTypes[i], depth + 1));
        }
        if (wrapWithBraces) {
            sb.append('}');
        }
        return sb.toString();
    }
}
