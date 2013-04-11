package edu.uci.ics.asterix.external.library.java;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.external.library.JTypeObjectFactory;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class JTypes {

    public static final class JInt implements IJObject {

        private AMutableInt32 value = new AMutableInt32(0);

        public void setValue(int v) {
            this.value.setValue(v);
        }

        public int getValue() {
            return value.getIntegerValue().intValue();
        }

        @Override
        public ATypeTag getTypeTag() {
            return value.getType().getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }
    }

    public static final class JString implements IJObject {

        private AMutableString value = new AMutableString("");

        public void setValue(String v) {
            this.value.setValue(v);
        }

        public String getValue() {
            return value.getStringValue();
        }

        @Override
        public ATypeTag getTypeTag() {
            return value.getType().getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }
    }

    public static final class JList implements IJObject {

        private AOrderedListType listType;
        private AMutableOrderedList value;

        public JList(AOrderedListType listType) {
            this.listType = listType;
            this.value = new AMutableOrderedList(listType);
        }

        public void add(IJObject element) {
            value.add(element.getIAObject());
        }

        @Override
        public ATypeTag getTypeTag() {
            return value.getType().getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }
    }

    public static final class JRecord implements IJObject {

        private AMutableRecord value;
        private byte[] recordBytes;
        private ARecordType recordType;
        private List<IJObject> fields;
        private List<String> fieldNames;
        private List<IAType> fieldTypes;
        private final IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<IJObject, IAType>(
                new JTypeObjectFactory());
        private int numFieldsAdded = 0;
        private List<Boolean> openField;

        private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
        private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

        public JRecord(ARecordType recordType) {
            this.recordType = recordType;
            this.fields = new ArrayList<IJObject>();
            initFieldInfo();
        }

        public JRecord(ARecordType recordType, IJObject[] fields) {
            this.recordType = recordType;
            this.fields = new ArrayList<IJObject>();
            for (IJObject jObject : fields) {
                this.fields.add(jObject);
            }
            initFieldInfo();
        }

        private void initFieldInfo() {
            this.openField = new ArrayList<Boolean>();
            fieldNames = new ArrayList<String>();
            for (String name : recordType.getFieldNames()) {
                fieldNames.add(name);
                openField.add(false);
            }
            fieldTypes = new ArrayList<IAType>();
            for (IAType type : recordType.getFieldTypes()) {
                fieldTypes.add(type);
            }

        }

        private IAObject[] getIAObjectArray(List<IJObject> fields) {
            IAObject[] retValue = new IAObject[fields.size()];
            int index = 0;
            for (IJObject jObject : fields) {
                retValue[index++] = getIAObject(jObject);
            }
            return retValue;
        }

        private IAObject getIAObject(IJObject jObject) {
            IAObject retVal = null;
            switch (jObject.getTypeTag()) {
                case INT32:
                case STRING:
                    retVal = jObject.getIAObject();
                    break;
                case RECORD:
                    ARecordType recType = ((JRecord) jObject).getRecordType();
                    IAObject[] fields = new IAObject[((JRecord) jObject).getFields().size()];
                    int index = 0;
                    for (IJObject field : ((JRecord) jObject).getFields()) {
                        fields[index++] = getIAObject(field);
                    }

                    retVal = new AMutableRecord(recType, fields);
            }
            return retVal;
        }

        public void addField(String fieldName, IJObject fieldValue) {
            int pos = getFieldPosByName(fieldName);
            if (pos >= 0) {
                throw new IllegalArgumentException("field already defined");
            }
            numFieldsAdded++;
            fields.add(fieldValue);
            fieldNames.add(fieldName);
            fieldTypes.add(fieldValue.getIAObject().getType());
            openField.add(true);
        }

        public IJObject getValueByName(String fieldName) throws AsterixException, IOException {
            int fieldPos = getFieldPosByName(fieldName);

            if (recordBytes == null) {
                IJObject jtype = getJObject(value.getValueByPos(fieldPos));
                fields.set(fieldPos, jtype);
                return jtype;
            }

            if (recordBytes[0] == SER_NULL_TYPE_TAG) {
                return null;
            }

            if (recordBytes[0] != SER_RECORD_TYPE_TAG) {
                throw new AsterixException("Field accessor is not defined for values of type"
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(recordBytes[0]));
            }

            int fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetById(recordBytes, fieldPos,
                    getNullBitMapSize(), recordType.isOpen());

            if (fieldValueOffset < 0) {
                return null;
            }

            IAType fieldValueType = recordType.getFieldTypes()[fieldPos];
            ATypeTag fieldValueTypeTag = null;
            int fieldValueLength = 0;
            if (fieldValueType.getTypeTag().equals(ATypeTag.UNION)) {
                if (NonTaggedFormatUtil.isOptionalField((AUnionType) fieldValueType)) {
                    fieldValueTypeTag = ((AUnionType) fieldValueType).getUnionList()
                            .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST).getTypeTag();
                    fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(recordBytes, fieldValueOffset,
                            fieldValueTypeTag, false);
                    //                    out.writeByte(fieldValueTypeTag.serialize());
                } else {
                    // union .. the general case
                    throw new NotImplementedException();
                }
            } else {
                fieldValueTypeTag = fieldValueType.getTypeTag();
                fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(recordBytes, fieldValueOffset,
                        fieldValueTypeTag, false);
                //                out.writeByte(fieldValueTypeTag.serialize());
            }

            IJObject fieldValue = getJType(fieldValueTypeTag, recordBytes, fieldValueOffset, fieldValueLength, fieldPos);
            fields.set(fieldPos, fieldValue);
            return fieldValue;
        }

        public void setValue(byte[] recordBytes) {
            this.recordBytes = recordBytes;
        }

        public void setValueAtPos(int pos, IJObject jtype) {
            fields.set(pos, jtype);
        }

        public void setValue(AMutableRecord mutableRecord) {
            this.value = mutableRecord;
            this.recordType = mutableRecord.getType();
        }

        @Override
        public ATypeTag getTypeTag() {
            return recordType.getTypeTag();
        }

        public void setField(String fieldName, IJObject fieldValue) {
            int pos = getFieldPosByName(fieldName);
            switch (fields.get(pos).getTypeTag()) {
                case INT32:
                    ((JInt) fields.get(pos)).setValue(((AMutableInt32) fieldValue.getIAObject()).getIntegerValue()
                            .intValue());
                    break;
                case STRING:
                    ((JString) fields.get(pos)).setValue(((AMutableString) fieldValue.getIAObject()).getStringValue());
                    break;
                case RECORD:
                    ((JRecord) fields.get(pos)).setValue(((AMutableRecord) fieldValue));
                    break;
            }
        }

        private int getFieldPosByName(String fieldName) {
            int index = 0;
            for (String name : fieldNames) {
                if (name.equals(fieldName)) {
                    return index;
                }
                index++;
            }
            return -1;
        }

        private IJObject getJType(ATypeTag typeTag, byte[] argument, int offset, int len, int fieldIndex)
                throws HyracksDataException {
            IJObject jObject;
            switch (typeTag) {
                case INT32: {
                    int v = valueFromBytes(argument, offset, len);
                    jObject = objectPool.allocate(BuiltinType.AINT32);
                    ((JInt) jObject).setValue(v);
                    break;

                }
                case STRING: {
                    String v = AStringSerializerDeserializer.INSTANCE.deserialize(
                            new DataInputStream(new ByteArrayInputStream(argument, offset, len))).getStringValue();
                    jObject = objectPool.allocate(BuiltinType.ASTRING);
                    ((JString) jObject).setValue(v);
                    break;
                }
                case RECORD:
                    ARecordType fieldRecordType = (ARecordType) recordType.getFieldTypes()[fieldIndex];
                    jObject = objectPool.allocate(fieldRecordType);
                    byte[] recBytes = new byte[len];
                    System.arraycopy(argument, offset, recBytes, 0, len);
                    ((JRecord) jObject).setValue(argument);
                    break;
                default:
                    throw new IllegalStateException("Argument type: " + typeTag);
            }
            return jObject;
        }

        private IJObject getJObject(IAObject iaobject) throws HyracksDataException {
            ATypeTag typeTag = iaobject.getType().getTypeTag();
            IJObject jtype;
            switch (typeTag) {
                case INT32: {
                    int v = ((AInt32) iaobject).getIntegerValue().intValue();
                    jtype = new JInt();
                    ((JInt) jtype).setValue(v);
                    break;
                }
                case STRING: {
                    jtype = new JString();
                    ((JString) jtype).setValue(((AString) iaobject).getStringValue());
                    break;
                }
                case RECORD:
                    ARecordType fieldRecordType = ((ARecord) iaobject).getType();
                    jtype = new JRecord(fieldRecordType);
                    ((JRecord) jtype).setValue((AMutableRecord) iaobject);
                    break;
                default:
                    throw new IllegalStateException("Argument type: " + typeTag);
            }
            return jtype;
        }

        private static int valueFromBytes(byte[] bytes, int offset, int length) {
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16)
                    + ((bytes[offset + 2] & 0xff) << 8) + ((bytes[offset + 3] & 0xff) << 0);
        }

        public ARecordType getRecordType() {
            return recordType;
        }

        public List<IJObject> getFields() {
            return fields;
        }

        @Override
        public IAObject getIAObject() {
            if (value == null || numFieldsAdded > 0) {
                value = new AMutableRecord(recordType, getIAObjectArray(fields));
            }
            return value;
        }

        public void close() {
            objectPool.reset();
            if (numFieldsAdded > 0) {
                int totalFields = fieldNames.size();
                for (int i = 0; i < numFieldsAdded; i++) {
                    fieldNames.remove(totalFields - 1 - i);
                    fieldTypes.remove(totalFields - 1 - i);
                    fields.remove(totalFields - 1 - i);
                }
                numFieldsAdded = 0;
            }
        }

        private int getNullBitMapSize() {
            int nullBitmapSize = 0;
            if (NonTaggedFormatUtil.hasNullableField(recordType)) {
                nullBitmapSize = (int) Math.ceil(recordType.getFieldNames().length / 8.0);
            } else {
                nullBitmapSize = 0;
            }
            return nullBitmapSize;
        }

        public List<Boolean> getOpenField() {
            return openField;
        }

        public List<String> getFieldNames() {
            return fieldNames;
        }

        public List<IAType> getFieldTypes() {
            return fieldTypes;
        }

    }
}