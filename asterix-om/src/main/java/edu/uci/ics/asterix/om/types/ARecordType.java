/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.types;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.annotations.IRecordTypeAnnotation;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

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
     */
    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen)
            throws AsterixException {
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
     */
    public int findFieldPosition(byte[] bytes, int start, int length) {
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
     * Validates the partitioning expression that will be used to partition a dataset.
     * 
     * @param partitioningExprs
     *            a list of partitioning expressions that will be validated
     * @throws AlgebricksException
     *             (if the validation failed), IOException
     */
    public void validatePartitioningExpressions(List<String> partitioningExprs) throws AlgebricksException, IOException {
        for (String fieldName : partitioningExprs) {
            IAType fieldType = getFieldType(fieldName);
            if (fieldType == null) {
                throw new AlgebricksException("A field with this name  \"" + fieldName + "\" could not be found.");
            }
            switch (fieldType.getTypeTag()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case DATE:
                case TIME:
                case DATETIME:
                case YEARMONTHDURATION:
                case DAYTIMEDURATION:
                    break;
                case UNION:
                    throw new AlgebricksException("The partitioning key \"" + fieldName + "\" cannot be nullable");
                default:
                    throw new AlgebricksException("The partitioning key \"" + fieldName + "\" cannot be of type "
                            + fieldType.getTypeTag() + ".");
            }
        }
    }

    /**
     * Validates the key fields that will be used as keys of an index.
     * 
     * @param keyFieldNames
     *            a list of key fields that will be validated
     * @param indexType
     *            the type of the index that its key fields is being validated
     * @throws AlgebricksException
     *             (if the validation failed), IOException
     */
    public void validateKeyFields(List<String> keyFieldNames, IndexType indexType) throws AlgebricksException,
            IOException {
        for (String fieldName : keyFieldNames) {
            IAType fieldType = getFieldType(fieldName);
            if (fieldType == null) {
                throw new AlgebricksException("A field with this name  \"" + fieldName + "\" could not be found.");
            }
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
                        case DATE:
                        case TIME:
                        case DATETIME:
                        case UNION:
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
                                    + fieldType.getTypeTag() + " cannot be indexed using the Length Partitioned N-Gram index.");
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
                                    + fieldType.getTypeTag() + " cannot be indexed using the Length Partitioned Keyword index.");
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
}
