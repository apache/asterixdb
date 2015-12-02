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

package org.apache.asterix.om.types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.annotations.IRecordTypeAnnotation;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.visitors.IOMVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ARecordType is read-only and shared by different partitions at runtime.
 * Note: to check whether a field name is defined in the closed part at runtime,
 * please use RuntimeRecordTypeInfo which separates the mutable states
 * from ARecordType and has to be one-per-partition.
 */
public class ARecordType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private final String[] fieldNames;
    private final IAType[] fieldTypes;
    private final Map<String, Integer> fieldNameToIndexMap = new HashMap<String, Integer>();
    private final boolean isOpen;
    private final List<IRecordTypeAnnotation> annotations = new ArrayList<IRecordTypeAnnotation>();

    /**
     * @param typeName
     *            the name of the type
     * @param fieldNames
     *            the names of the closed fields
     * @param fieldTypes
     *            the types of the closed fields
     * @param isOpen
     *            whether the record is open
     * @throws AsterixException
     *             if there are duplicate field names or if there is an error serializing the field names
     * @throws HyracksDataException
     */
    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen) {
        super(typeName);
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isOpen = isOpen;

        // Puts field names to the field name to field index map.
        for (int index = 0; index < fieldNames.length; ++index) {
            fieldNameToIndexMap.put(fieldNames[index], index);
        }
    }

    public final String[] getFieldNames() {
        return fieldNames;
    }

    public final IAType[] getFieldTypes() {
        return fieldTypes;
    }

    public List<IRecordTypeAnnotation> getAnnotations() {
        return annotations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(typeName + ": ");
        if (isOpen) {
            sb.append("open ");
        } else {
            sb.append("closed ");
        }
        sb.append("{\n");
        int n = fieldNames.length;
        for (int i = 0; i < n; i++) {
            sb.append("  " + fieldNames[i] + ": " + fieldTypes[i].toString());
            if (i < n - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.RECORD;
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Returns the position of the field in the closed schema or -1 if the field does not exist.
     *
     * @param fieldName
     *            the name of the field whose position is sought
     * @return the position of the field in the closed schema or -1 if the field does not exist.
     */
    public int getFieldIndex(String fieldName) throws IOException {
        if (fieldNames == null) {
            return -1;
        }
        Integer index = fieldNameToIndexMap.get(fieldName);
        if (index == null) {
            return -1;
        } else {
            return index;
        }
    }

    /**
     * @param subFieldName
     *            The full pathname of the child
     * @param parent
     *            The type of the parent
     * @return the type of the child
     * @throws IOException
     */

    public IAType getSubFieldType(List<String> subFieldName, IAType parent) throws IOException {
        ARecordType subRecordType = (ARecordType) parent;
        for (int i = 0; i < subFieldName.size() - 1; i++) {
            subRecordType = (ARecordType) subRecordType.getFieldType(subFieldName.get(i));
        }
        return subRecordType.getFieldType(subFieldName.get(subFieldName.size() - 1));
    }

    /**
     * @param subFieldName
     *            The full pathname of the child
     * @return the type of the child
     * @throws IOException
     */
    public IAType getSubFieldType(List<String> subFieldName) throws IOException {
        IAType subRecordType = getFieldType(subFieldName.get(0));
        for (int i = 1; i < subFieldName.size(); i++) {
            if (subRecordType == null) {
                return null;
            }
            if (subRecordType.getTypeTag().equals(ATypeTag.UNION)) {
                //enforced SubType
                subRecordType = ((AUnionType) subRecordType).getNullableType();
                if (subRecordType.getTypeTag().serialize() != ATypeTag.RECORD.serialize()) {
                    throw new IOException(
                            "Field accessor is not defined for values of type " + subRecordType.getTypeTag());
                }

            }
            subRecordType = ((ARecordType) subRecordType).getFieldType(subFieldName.get(i));
        }
        return subRecordType;
    }

    /**
     * Returns the field type of the field name if it exists, otherwise null.
     *
     * @param fieldName
     *            the fieldName whose type is sought
     * @return the field type of the field name if it exists, otherwise null
     * @throws IOException
     *             if an error occurs while serializing the field name
     */
    public IAType getFieldType(String fieldName) throws IOException {
        int fieldPos = getFieldIndex(fieldName);
        if (fieldPos < 0 || fieldPos >= fieldTypes.length) {
            return null;
        }
        return fieldTypes[fieldPos];
    }

    /**
     * Returns true or false indicating whether or not a field is closed.
     *
     * @param fieldName
     *            the name of the field to check
     * @return true if fieldName is a closed field, otherwise false
     * @throws IOException
     */
    public boolean isClosedField(String fieldName) throws IOException {
        return getFieldIndex(fieldName) != -1;
    }

    public boolean doesFieldExist(String fieldName) {
        for (String f : fieldNames) {
            if (f.compareTo(fieldName) == 0) {
                return true;
            }
        }
        return false;
    }

    public ARecordType deepCopy(ARecordType type) throws AlgebricksException {
        IAType[] newTypes = new IAType[type.fieldNames.length];
        for (int i = 0; i < type.fieldTypes.length; i++) {
            if (type.fieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                newTypes[i] = deepCopy((ARecordType) type.fieldTypes[i]);
            } else {
                newTypes[i] = type.fieldTypes[i];
            }
        }
        return new ARecordType(type.typeName, type.fieldNames, newTypes, type.isOpen);
    }

    @Override
    public String getDisplayName() {
        return "ARecord";
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    @Override
    public void generateNestedDerivedTypeNames() {
        for (int i = 0; i < fieldTypes.length; i++) {
            IAType fieldType = fieldTypes[i];
            if (fieldType.getTypeTag().isDerivedType() && fieldType.getTypeName() == null) {
                AbstractComplexType nestedType = ((AbstractComplexType) fieldType);
                nestedType.setTypeName(getTypeName() + "_" + fieldNames[i]);
                nestedType.generateNestedDerivedTypeNames();
            }
        }
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ARecordType)) {
            return false;
        }
        ARecordType rt = (ARecordType) obj;
        return isOpen == rt.isOpen && Arrays.deepEquals(fieldNames, rt.fieldNames)
                && Arrays.deepEquals(fieldTypes, rt.fieldTypes);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fieldNames.length; i++) {
            h += 31 * h + fieldNames[i].hashCode();
        }
        for (int i = 0; i < fieldTypes.length; i++) {
            h += 31 * h + fieldTypes[i].hashCode();
        }
        return h;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject type = new JSONObject();
        type.put("type", ARecordType.class.getName());
        type.put("name", typeName);
        if (isOpen) {
            type.put("open", true);
        } else {
            type.put("open", false);
        }

        JSONArray fields = new JSONArray();
        for (int i = 0; i < fieldNames.length; i++) {
            JSONObject field = new JSONObject();
            field.put(fieldNames[i], fieldTypes[i].toJSON());
            fields.put(field);
        }

        type.put("fields", fields);
        return type;
    }

    public static int computeNullBitmapSize(ARecordType rt) {
        return NonTaggedFormatUtil.hasNullableField(rt) ? (int) Math.ceil(rt.getFieldNames().length / 8.0) : 0;
    }

}
