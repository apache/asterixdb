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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.annotations.IRecordTypeAnnotation;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.visitors.IOMVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class ARecordType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private String[] fieldNames;
    private IAType[] fieldTypes;
    private boolean isOpen;
    private final List<IRecordTypeAnnotation> annotations = new ArrayList<IRecordTypeAnnotation>();

    private transient IBinaryHashFunction fieldNameHashFunction;
    private transient IBinaryComparator fieldNameComparator;
    private final byte serializedFieldNames[];
    private final int serializedFieldNameOffsets[];
    private final long hashCodeIndexPairs[];

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
    @SuppressWarnings("resource")
    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen)
            throws AsterixException, HyracksDataException {
        super(typeName);
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isOpen = isOpen;

        fieldNameComparator = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY)
                .createBinaryComparator();
        fieldNameHashFunction = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                .createBinaryHashFunction();
        ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        DataOutputStream dos = new DataOutputStream(baaos);
        serializedFieldNameOffsets = new int[fieldNames.length];
        hashCodeIndexPairs = new long[fieldNames.length];

        int length = 0;
        for (int i = 0; i < fieldNames.length; i++) {
            serializedFieldNameOffsets[i] = baaos.size();
            try {
                dos.writeUTF(fieldNames[i]);
            } catch (IOException e) {
                throw new AsterixException(e);
            }
            length = baaos.size() - serializedFieldNameOffsets[i];
            hashCodeIndexPairs[i] = fieldNameHashFunction.hash(baaos.getByteArray(), serializedFieldNameOffsets[i],
                    length);
            hashCodeIndexPairs[i] = hashCodeIndexPairs[i] << 32;
            hashCodeIndexPairs[i] = hashCodeIndexPairs[i] | i;
        }
        try {
            dos.close();
        } catch (IOException e) {
            throw new AsterixException(e);
        }
        serializedFieldNames = baaos.getByteArray();

        Arrays.sort(hashCodeIndexPairs);
        int j;
        for (int i = 0; i < fieldNames.length; i++) {
            j = findFieldPosition(serializedFieldNames, serializedFieldNameOffsets[i],
                    UTF8StringPointable.getStringLength(serializedFieldNames, serializedFieldNameOffsets[i]));
            if (j != i) {
                throw new AsterixException("Closed fields " + j + " and " + i + " have the same field name \""
                        + fieldNames[i] + "\"");
            }
        }
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        fieldNameComparator = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY)
                .createBinaryComparator();
        fieldNameHashFunction = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                .createBinaryHashFunction();
    }

    /**
     * Returns the position of the field in the closed schema or -1 if the field does not exist.
     *
     * @param bytes
     *            the serialized bytes of the field name
     * @param start
     *            the starting offset of the field name in bytes
     * @param length
     *            the length of the field name in bytes
     * @return the position of the field in the closed schema or -1 if the field does not exist.
     * @throws HyracksDataException
     */
    public int findFieldPosition(byte[] bytes, int start, int length) throws HyracksDataException {
        if (hashCodeIndexPairs.length == 0) {
            return -1;
        }

        int fIndex;
        int probeFieldHash = fieldNameHashFunction.hash(bytes, start, length);
        int i = Arrays.binarySearch(hashCodeIndexPairs, ((long) probeFieldHash) << 32);
        i = (i < 0) ? (i = -1 * (i + 1)) : i;

        while (i < hashCodeIndexPairs.length && (int) (hashCodeIndexPairs[i] >>> 32) == probeFieldHash) {
            fIndex = (int) hashCodeIndexPairs[i];
            int cFieldLength = UTF8StringPointable.getStringLength(serializedFieldNames,
                    serializedFieldNameOffsets[fIndex]);
            if (fieldNameComparator.compare(serializedFieldNames, serializedFieldNameOffsets[fIndex], cFieldLength,
                    bytes, start, length) == 0) {
                return fIndex;
            }
            i++;
        }

        return -1;
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
    public int findFieldPosition(String fieldName) throws IOException {
        ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        DataOutputStream dos = new DataOutputStream(baaos);
        UTF8StringSerializerDeserializer.INSTANCE.serialize(fieldName, dos);
        return findFieldPosition(baaos.getByteArray(), 0, baaos.getByteArray().length);
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
                    throw new IOException("Field accessor is not defined for values of type "
                            + subRecordType.getTypeTag());
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
        int fieldPos = findFieldPosition(fieldName);
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
        return findFieldPosition(fieldName) != -1;
    }

    /**
     * Validates the partitioning expression that will be used to partition a dataset and returns expression type.
     *
     * @param partitioningExprs
     *            a list of partitioning expressions that will be validated
     * @return a list of partitioning expressions types
     * @throws AlgebricksException
     *             (if the validation failed), IOException
     */
    public List<IAType> validatePartitioningExpressions(List<List<String>> partitioningExprs, boolean autogenerated)
            throws AsterixException, IOException {
        List<IAType> partitioningExprTypes = new ArrayList<IAType>(partitioningExprs.size());
        if (autogenerated) {
            if (partitioningExprs.size() > 1) {
                throw new AsterixException("Cannot autogenerate a composite primary key");
            }
            List<String> fieldName = partitioningExprs.get(0);
            IAType fieldType = getSubFieldType(fieldName);
            partitioningExprTypes.add(fieldType);

            ATypeTag pkTypeTag = fieldType.getTypeTag();
            if (pkTypeTag != ATypeTag.UUID) {
                throw new AsterixException("Cannot autogenerate a primary key for type " + pkTypeTag
                        + ". Autogenerated primary keys must be of type " + ATypeTag.UUID + ".");
            }
        } else {
            for (int i = 0; i < partitioningExprs.size(); i++) {
                List<String> fieldName = partitioningExprs.get(i);
                IAType fieldType = getSubFieldType(fieldName);

                switch (fieldType.getTypeTag()) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                    case DOUBLE:
                    case STRING:
                    case BINARY:
                    case DATE:
                    case TIME:
                    case UUID:
                    case DATETIME:
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                        partitioningExprTypes.add(fieldType);
                        break;
                    case UNION:
                        throw new AsterixException("The partitioning key \"" + fieldName + "\" cannot be nullable");
                    default:
                        throw new AsterixException("The partitioning key \"" + fieldName + "\" cannot be of type "
                                + fieldType.getTypeTag() + ".");
                }
            }
        }
        return partitioningExprTypes;
    }

    private IAType getPartitioningExpressionType(String fieldName, boolean autogenerated) throws AsterixException,
            IOException {
        IAType fieldType = getFieldType(fieldName);
        if (fieldType == null) {
            if (autogenerated) {
                throw new AsterixException("Primary key field: " + fieldName
                        + " should be defined in the type that the dataset is using.");
            } else {
                throw new AsterixException("Primary key field: " + fieldName + " could not be found.");
            }
        }
        return fieldType;
    }

    /**
     * Validates the key fields that will be used as keys of an index.
     *
     * @param keyFieldNames
     *            a map of key fields that will be validated
     * @param keyFieldTypes
     *            a map of key types (if provided) that will be validated
     * @param indexType
     *            the type of the index that its key fields is being validated
     * @throws AlgebricksException
     *             (if the validation failed), IOException
     */
    public void validateKeyFields(List<List<String>> keyFieldNames, List<IAType> keyFieldTypes, IndexType indexType)
            throws AlgebricksException, IOException {
        int pos = 0;
        boolean openFieldCompositeIdx = false;
        for (List<String> fieldName : keyFieldNames) {
            IAType fieldType = getSubFieldType(fieldName);
            if (fieldType == null) {
                fieldType = keyFieldTypes.get(pos);
                if (keyFieldTypes.get(pos) == BuiltinType.ANULL)
                    throw new AlgebricksException("A field with this name  \"" + fieldName + "\" could not be found.");
            } else if (openFieldCompositeIdx)
                throw new AlgebricksException("A closed field \"" + fieldName
                        + "\" could be only in a prefix part of the composite index, containing opened field.");
            if (keyFieldTypes.get(pos) != BuiltinType.ANULL
                    && fieldType.getTypeTag() != keyFieldTypes.get(pos).getTypeTag())
                throw new AlgebricksException("A field \"" + fieldName + "\" is already defined with the type \""
                        + fieldType + "\"");
            switch (indexType) {
                case BTREE:
                    switch (fieldType.getTypeTag()) {
                        case INT8:
                        case INT16:
                        case INT32:
                        case INT64:
                        case FLOAT:
                        case DOUBLE:
                        case STRING:
                        case BINARY:
                        case DATE:
                        case TIME:
                        case DATETIME:
                        case UNION:
                        case UUID:
                        case YEARMONTHDURATION:
                        case DAYTIMEDURATION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag() + " cannot be indexed using the BTree index.");
                    }
                    break;
                case RTREE:
                    switch (fieldType.getTypeTag()) {
                        case POINT:
                        case LINE:
                        case RECTANGLE:
                        case CIRCLE:
                        case POLYGON:
                        case UNION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag() + " cannot be indexed using the RTree index.");
                    }
                    break;
                case LENGTH_PARTITIONED_NGRAM_INVIX:
                    switch (fieldType.getTypeTag()) {
                        case STRING:
                        case UNION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag()
                                    + " cannot be indexed using the Length Partitioned N-Gram index.");
                    }
                    break;
                case LENGTH_PARTITIONED_WORD_INVIX:
                    switch (fieldType.getTypeTag()) {
                        case STRING:
                        case UNORDEREDLIST:
                        case ORDEREDLIST:
                        case UNION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag()
                                    + " cannot be indexed using the Length Partitioned Keyword index.");
                    }
                    break;
                case SINGLE_PARTITION_NGRAM_INVIX:
                    switch (fieldType.getTypeTag()) {
                        case STRING:
                        case UNION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag() + " cannot be indexed using the N-Gram index.");
                    }
                    break;
                case SINGLE_PARTITION_WORD_INVIX:
                    switch (fieldType.getTypeTag()) {
                        case STRING:
                        case UNORDEREDLIST:
                        case ORDEREDLIST:
                        case UNION:
                            break;
                        default:
                            throw new AlgebricksException("The field \"" + fieldName + "\" which is of type "
                                    + fieldType.getTypeTag() + " cannot be indexed using the Keyword index.");
                    }
                    break;
                default:
                    throw new AlgebricksException("Invalid index type: " + indexType + ".");
            }
            pos++;
        }
    }

    /**
     * Validates the field that will be used as filter for the components of an LSM index.
     *
     * @param keyFieldNames
     *            a list of key fields that will be validated
     * @param indexType
     *            the type of the index that its key fields is being validated
     * @throws AlgebricksException
     *             (if the validation failed), IOException
     */
    public void validateFilterField(List<String> filterField) throws AlgebricksException, IOException {
        IAType fieldType = getSubFieldType(filterField);
        if (fieldType == null) {
            throw new AlgebricksException("A field with this name  \"" + filterField + "\" could not be found.");
        }
        switch (fieldType.getTypeTag()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BINARY:
            case DATE:
            case TIME:
            case DATETIME:
            case UUID:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
                break;
            case UNION:
                throw new AlgebricksException("The filter field \"" + filterField + "\" cannot be nullable");
            default:
                throw new AlgebricksException("The field \"" + filterField + "\" which is of type "
                        + fieldType.getTypeTag() + " cannot be used as a filter for a dataset.");
        }
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
        try {
            return new ARecordType(type.typeName, type.fieldNames, newTypes, type.isOpen);
        } catch (AsterixException | HyracksException e) {
            throw new AlgebricksException(e);
        }
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
            h += 31 * h + (int) (hashCodeIndexPairs[i] >> 32);
        }
        for (int i = 0; i < fieldTypes.length; i++) {
            h += 31 * h + fieldTypes[i].hashCode();
        }
        return h;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject type = new JSONObject();
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
