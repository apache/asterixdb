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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.IRecordTypeAnnotation;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ARecordType is read-only and shared by different partitions at runtime.
 * Note: to check whether a field name is defined in the closed part at runtime,
 * please use RuntimeRecordTypeInfo which separates the mutable states
 * from ARecordType and has to be one-per-partition.
 */
public class ARecordType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private static final JavaType SET = OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class);
    private static final String IS_OPEN = "isOpen";
    private static final String FIELD_NAMES = "fieldNames";
    private static final String FIELD_TYPES = "fieldTypes";
    private static final String ADDITIONAL_FIELDS = "additionalFieldNames";
    private final String[] fieldNames;
    private final IAType[] fieldTypes;
    private final Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
    private final boolean isOpen;
    private final transient List<IRecordTypeAnnotation> annotations = new ArrayList<>();
    // The following set of names do not belong to the closed part,
    // but are additional possible field names. For example, a field "foo" with type
    // ANY cannot be in the closed part, but "foo" is a possible field name.
    // This is used for resolve a field access with prefix path missing.
    // If allPossibleAdditionalFieldNames is null, that means compiler does not know
    // the bounded set of all possible additional field names.
    private final Set<String> allPossibleAdditionalFieldNames;

    /**
     * @param typeName
     *            the name of the type
     * @param fieldNames
     *            the names of the closed fields
     * @param fieldTypes
     *            the types of the closed fields
     * @param isOpen
     *            whether the record is open
     */
    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen) {
        this(typeName, fieldNames, fieldTypes, isOpen, null);
    }

    /**
     * @param typeName
     *            the name of the type
     * @param fieldNames
     *            the names of the closed fields
     * @param fieldTypes
     *            the types of the closed fields
     * @param isOpen
     *            whether the record is open
     * @param allPossibleAdditionalFieldNames,
     *            all possible additional field names.
     */
    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen,
            Set<String> allPossibleAdditionalFieldNames) {
        super(typeName);
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isOpen = isOpen;

        // Puts field names to the field name to field index map.
        for (int index = 0; index < fieldNames.length; ++index) {
            fieldNameToIndexMap.put(fieldNames[index], index);
        }
        this.allPossibleAdditionalFieldNames = allPossibleAdditionalFieldNames;
    }

    public boolean canContainField(String fieldName) {
        if (this.isOpen && allPossibleAdditionalFieldNames == null) {
            // An open record type without information on possible additional fields can potentially contain
            // a field with any name.
            return true;
        }
        if (isOpen) {
            // An open record type with information on possible additional fields can determine whether
            // a field can potentially be contained in a record.
            return fieldNameToIndexMap.containsKey(fieldName) || allPossibleAdditionalFieldNames.contains(fieldName);
        } else {
            return fieldNameToIndexMap.containsKey(fieldName);
        }
    }

    public boolean knowsAllPossibleAdditonalFieldNames() {
        return allPossibleAdditionalFieldNames != null;
    }

    public Set<String> getAllPossibleAdditonalFieldNames() {
        return allPossibleAdditionalFieldNames;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public IAType[] getFieldTypes() {
        return fieldTypes;
    }

    public List<IRecordTypeAnnotation> getAnnotations() {
        return annotations;
    }

    @Override
    public String toString() {
        return append(new StringBuilder()).toString();
    }

    private StringBuilder append(StringBuilder sb) {
        if (typeName != null) {
            sb.append(typeName).append(": ");
        }
        sb.append(isOpen ? "open" : "closed");
        sb.append(" {\n");
        int n = fieldNames.length;
        for (int i = 0; i < n; i++) {
            sb.append("  ").append(fieldNames[i]).append(": ").append(fieldTypes[i]);
            sb.append(i < (n - 1) ? ",\n" : "\n");
        }
        return sb.append("}\n");
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.OBJECT;
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
    public int getFieldIndex(String fieldName) {
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
     */

    public IAType getSubFieldType(List<String> subFieldName, IAType parent) {
        ARecordType subRecordType = (ARecordType) parent;
        for (int i = 0; i < (subFieldName.size() - 1); i++) {
            subRecordType = (ARecordType) subRecordType.getFieldType(subFieldName.get(i));
        }
        return subRecordType.getFieldType(subFieldName.get(subFieldName.size() - 1));
    }

    /**
     * @param subFieldName
     *            The full pathname of the child
     * @return the type of the child
     * @throws AsterixException
     */
    public IAType getSubFieldType(List<String> subFieldName) throws AlgebricksException {
        IAType subRecordType = getFieldType(subFieldName.get(0));
        for (int i = 1; i < subFieldName.size(); i++) {
            if (subRecordType == null) {
                return null;
            }
            if (subRecordType.getTypeTag().equals(ATypeTag.UNION)) {
                //enforced SubType
                subRecordType = ((AUnionType) subRecordType).getActualType();
                if (subRecordType.getTypeTag() != ATypeTag.OBJECT) {
                    throw new AsterixException(
                            "Field accessor is not defined for values of type " + subRecordType.getTypeTag());
                }

            }
            subRecordType = ((ARecordType) subRecordType).getFieldType(subFieldName.get(i));
        }
        return subRecordType;
    }

    /**
     *
     * @param subFieldName
     *            The full pathname of the field
     * @return The nullability of the field
     * @throws AlgebricksException
     */
    public boolean isSubFieldNullable(List<String> subFieldName) throws AlgebricksException {
        IAType subRecordType = getFieldType(subFieldName.get(0));
        for (int i = 1; i < subFieldName.size(); i++) {
            if (subRecordType == null) {
                // open field is nullable
                return true;
            }
            if (subRecordType.getTypeTag().equals(ATypeTag.UNION)) {
                if (NonTaggedFormatUtil.isOptional(subRecordType)) {
                    return true;
                }
                subRecordType = ((AUnionType) subRecordType).getActualType();
                if (subRecordType.getTypeTag() != ATypeTag.OBJECT) {
                    throw new AsterixException(
                            "Field accessor is not defined for values of type " + subRecordType.getTypeTag());
                }
            }
            if (!(subRecordType instanceof ARecordType)) {
                throw CompilationException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Illegal field type " + subRecordType.getTypeTag() + " when checking field nullability");
            }
            subRecordType = ((ARecordType) subRecordType).getFieldType(subFieldName.get(i));
        }
        return subRecordType == null || NonTaggedFormatUtil.isOptional(subRecordType);
    }

    /**
     * Returns the field type of the field name if it exists, otherwise null.
     *
     * @param fieldName
     *            the fieldName whose type is sought
     * @return the field type of the field name if it exists, otherwise null
     *         NOTE: this method doesn't work for nested fields
     */
    public IAType getFieldType(String fieldName) {
        int fieldPos = getFieldIndex(fieldName);
        if ((fieldPos < 0) || (fieldPos >= fieldTypes.length)) {
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
     */
    public boolean isClosedField(String fieldName) {
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

    public ARecordType deepCopy(ARecordType type) {
        IAType[] newTypes = new IAType[type.fieldNames.length];
        for (int i = 0; i < type.fieldTypes.length; i++) {
            if (type.fieldTypes[i].getTypeTag() == ATypeTag.OBJECT) {
                newTypes[i] = deepCopy((ARecordType) type.fieldTypes[i]);
            } else {
                newTypes[i] = type.fieldTypes[i];
            }
        }
        return new ARecordType(type.typeName, type.fieldNames, newTypes, type.isOpen);
    }

    @Override
    public String getDisplayName() {
        return "object";
    }

    @Override
    public IAType getType() {
        return BuiltinType.ALL_TYPE;
    }

    @Override
    public void generateNestedDerivedTypeNames() {
        for (int i = 0; i < fieldTypes.length; i++) {
            IAType fieldType = fieldTypes[i];
            if (fieldType.getTypeTag().isDerivedType() && (fieldType.getTypeName() == null)) {
                AbstractComplexType nestedType = (AbstractComplexType) fieldType;
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
        return (isOpen == rt.isOpen) && Arrays.deepEquals(fieldNames, rt.fieldNames)
                && Arrays.deepEquals(fieldTypes, rt.fieldTypes);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fieldNames.length; i++) {
            h += (31 * h) + fieldNames[i].hashCode();
        }
        for (int i = 0; i < fieldTypes.length; i++) {
            h += (31 * h) + fieldTypes[i].hashCode();
        }
        return h;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode type = om.createObjectNode();
        type.put("type", ARecordType.class.getName());
        type.put("name", typeName);
        if (isOpen) {
            type.put("open", true);
        } else {
            type.put("open", false);
        }

        ArrayNode fields = om.createArrayNode();
        for (int i = 0; i < fieldNames.length; i++) {
            ObjectNode field = om.createObjectNode();
            field.set(fieldNames[i], fieldTypes[i].toJSON());
            fields.add(field);
        }

        type.set("fields", fields);
        return type;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        addToJson(jsonObject);
        jsonObject.put(IS_OPEN, isOpen);
        jsonObject.putPOJO(FIELD_NAMES, fieldNames);
        jsonObject.putPOJO(ADDITIONAL_FIELDS, allPossibleAdditionalFieldNames);
        ArrayNode fieldTypesArray = OBJECT_MAPPER.createArrayNode();
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldTypesArray.add(fieldTypes[i].toJson(registry));
        }
        jsonObject.set(FIELD_TYPES, fieldTypesArray);
        return jsonObject;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        String typeName = json.get(TYPE_NAME_FIELD).asText();
        boolean isOpen = json.get(IS_OPEN).asBoolean();
        String[] fieldNames = OBJECT_MAPPER.convertValue(json.get(FIELD_NAMES), String[].class);
        Set<String> additionalFields = OBJECT_MAPPER.convertValue(json.get(ADDITIONAL_FIELDS), SET);
        ArrayNode fieldTypesNode = (ArrayNode) json.get(FIELD_TYPES);
        IAType[] fieldTypes = new IAType[fieldTypesNode.size()];
        for (int i = 0; i < fieldTypesNode.size(); i++) {
            fieldTypes[i] = (IAType) registry.deserialize(fieldTypesNode.get(i));
        }
        return new ARecordType(typeName, fieldNames, fieldTypes, isOpen, additionalFields);
    }

    public List<IAType> getFieldTypes(List<List<String>> fields) throws AlgebricksException {
        List<IAType> typeList = new ArrayList<>();
        for (List<String> field : fields) {
            typeList.add(getSubFieldType(field));
        }
        return typeList;
    }

    @Override
    public boolean containsType(IAType type) {
        for (IAType aType : fieldTypes) {
            if (aType.getTypeName().equals(type.getTypeName())) {
                return true;
            }
        }
        return false;
    }
}
