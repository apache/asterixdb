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
package org.apache.asterix.column.util;

import java.util.List;

import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.projection.DataProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ColumnSecondaryIndexSchemaUtil {
    private ColumnSecondaryIndexSchemaUtil() {
    }

    /**
     * Get the expected type for a value index
     *
     * @param paths the path to the indexed value
     * @return expected type
     */
    public static ARecordType getRecordType(List<List<String>> paths) throws AlgebricksException {
        ARecordType result = DataProjectionFiltrationInfo.EMPTY_TYPE;
        for (List<String> path : paths) {
            ARecordType type = getRecordType(path, "root", 0, BuiltinType.ANY);
            result = (ARecordType) RecordMergeTypeComputer.merge(result, type);
        }

        // Rename
        return new ARecordType("root", result.getFieldNames(), result.getFieldTypes(), true);
    }

    public static ARecordType getRecordTypeWithFieldTypes(List<List<String>> paths, List<IAType> types)
            throws AlgebricksException {
        ARecordType result = DataProjectionFiltrationInfo.EMPTY_TYPE;
        for (int i = 0; i < paths.size(); i++) {
            List<String> path = paths.get(i);
            ARecordType type = getRecordType(path, "root", 0, types.get(i));
            result = (ARecordType) RecordMergeTypeComputer.merge(result, type);
        }

        return new ARecordType("root", result.getFieldNames(), result.getFieldTypes(), true);
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

}
