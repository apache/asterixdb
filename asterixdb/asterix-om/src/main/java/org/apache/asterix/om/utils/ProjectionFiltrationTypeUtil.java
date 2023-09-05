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
package org.apache.asterix.om.utils;

import java.util.List;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ProjectionFiltrationTypeUtil {
    //Default open record type when requesting the entire fields
    public static final ARecordType ALL_FIELDS_TYPE = createType("");
    //Default open record type when requesting none of the fields
    public static final ARecordType EMPTY_TYPE = createType("{}");

    private ProjectionFiltrationTypeUtil() {
    }

    /**
     * Get the expected type for a value index
     *
     * @param paths the path to the indexed value
     * @return expected type
     */
    public static ARecordType getRecordType(List<List<String>> paths) throws AlgebricksException {
        ARecordType result = EMPTY_TYPE;
        for (List<String> path : paths) {
            ARecordType type = getPathRecordType(path);
            result = (ARecordType) RecordMergeTypeComputer.merge(result, type);
        }

        // Rename
        return new ARecordType("root", result.getFieldNames(), result.getFieldTypes(), true);
    }

    public static ARecordType getRecordTypeWithFieldTypes(List<List<String>> paths, List<IAType> types)
            throws AlgebricksException {
        ARecordType result = EMPTY_TYPE;
        for (int i = 0; i < paths.size(); i++) {
            List<String> path = paths.get(i);
            ARecordType type = getPathRecordType(path, types.get(i));
            result = (ARecordType) RecordMergeTypeComputer.merge(result, type);
        }

        return new ARecordType("root", result.getFieldNames(), result.getFieldTypes(), true);
    }

    public static ARecordType getPathRecordType(List<String> path) {
        return getRecordType(path, "root", 0, BuiltinType.ANY);
    }

    public static ARecordType getMergedPathRecordType(ARecordType previousType, List<String> path, IAType leafType)
            throws AlgebricksException {
        ARecordType type = getRecordType(path, "root", 0, leafType);
        if (previousType == EMPTY_TYPE) {
            return type;
        }
        return (ARecordType) RecordMergeTypeComputer.merge(previousType, type);
    }

    /**
     * Get the expected type for an array index
     *
     * @param unnest  the unnest path (UNNEST)
     * @param project the project path (SELECT)
     * @return the expected type
     */
    public static ARecordType getRecordType(List<List<String>> unnest, List<List<String>> project)
            throws AlgebricksException {
        IAType result = getLeafType(project);
        for (int i = unnest.size() - 1; i >= 0; i--) {
            List<String> path = unnest.get(i);
            result = getRecordType(path, "parent_" + i, 0, new AOrderedListType(result, "array_" + i));
        }
        return (ARecordType) result;
    }

    /**
     * Merge all types into a single one
     *
     * @param types the list of type for indexed fields
     * @return a single type that encompasses all indexed fields
     */
    public static ARecordType merge(List<ARecordType> types) throws AlgebricksException {
        ARecordType result = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            result = (ARecordType) RecordMergeTypeComputer.merge(result, types.get(i));
        }

        // Rename
        return new ARecordType("root", result.getFieldNames(), result.getFieldTypes(), true);
    }

    public static IAType renameType(IAType type, String name) {
        return new RenamedType(type, name);
    }

    public static RenamedType renameType(IAType type, int index) {
        return new RenamedType(type, String.valueOf(index), index);
    }

    private static ARecordType getPathRecordType(List<String> path, IAType type) {
        return getRecordType(path, "root", 0, type);
    }

    private static IAType getType(String typeName, List<String> path, int fieldIndex, IAType leafType) {
        if (fieldIndex == path.size()) {
            return leafType;
        }
        return getRecordType(path, typeName, fieldIndex, leafType);
    }

    private static ARecordType getRecordType(List<String> path, String typeName, int fieldIndex, IAType leafType) {
        String[] fieldNames = new String[1];
        IAType[] fieldTypes = new IAType[1];

        String fieldName = path.get(fieldIndex);
        fieldNames[0] = fieldName;
        fieldTypes[0] = getType(getTypeName(fieldName), path, fieldIndex + 1, leafType);
        return new ARecordType(typeName, fieldNames, fieldTypes, true);
    }

    private static IAType getLeafType(List<List<String>> project) throws AlgebricksException {
        IAType itemType;
        // Sometimes 'project' contains a single null value
        if (project.isEmpty() || project.get(0) == null) {
            itemType = BuiltinType.ANY;
        } else {
            itemType = getRecordType(project);
        }

        return itemType;
    }

    private static String getTypeName(String fieldName) {
        return fieldName + "_Type";
    }

    private static ARecordType createType(String typeName) {
        return new ARecordType(typeName, new String[] {}, new IAType[] {}, true);
    }

    public static class RenamedType implements IAType {
        private static final long serialVersionUID = 992690669300951839L;
        private final IAType originalType;
        private final String name;
        private final int index;

        RenamedType(IAType originalType, String name) {
            this(originalType, name, -1);
        }

        RenamedType(IAType originalType, String name, int index) {
            this.originalType = originalType;
            this.name = name;
            this.index = index;
        }

        @Override
        public IAType getType() {
            return originalType.getType();
        }

        @Override
        public boolean deepEqual(IAObject obj) {
            return originalType.deepEqual(obj);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof RenamedType) {
                return originalType.equals(((RenamedType) obj).originalType);
            }
            return originalType.equals(obj);
        }

        @Override
        public int hash() {
            return originalType.hash();
        }

        @Override
        public ATypeTag getTypeTag() {
            return originalType.getTypeTag();
        }

        @Override
        public String getDisplayName() {
            return originalType.getDisplayName();
        }

        @Override
        public String getTypeName() {
            return name;
        }

        @Override
        public <R, T> R accept(IATypeVisitor<R, T> visitor, T arg) {
            return visitor.visitFlat(this, arg);
        }

        @Override
        public ObjectNode toJSON() {
            return originalType.toJSON();
        }

        @Override
        public String toString() {
            return originalType.toString();
        }

        public int getIndex() {
            return index;
        }
    }

}
