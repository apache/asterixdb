/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.library;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjectUtil;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleDataInputStream;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleInputStream;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class JavaFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private IJObject[] arguments;
    private IJObject resultHolder;
    private ISerializerDeserializer resultSerde;
    private IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<IJObject, IAType>(new JTypeObjectFactory());
    byte[] buffer = new byte[32768];
    ByteArrayAccessibleInputStream bis = new ByteArrayAccessibleInputStream(buffer, 0, buffer.length);
    ByteArrayAccessibleDataInputStream dis = new ByteArrayAccessibleDataInputStream(bis);

    public JavaFunctionHelper(IExternalFunctionInfo finfo, IDataOutputProvider outputProvider)
            throws AlgebricksException {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        List<IAType> params = finfo.getParamList();
        arguments = new IJObject[params.size()];
        int index = 0;
        for (IAType param : params) {
            this.arguments[index] = objectPool.allocate(param);
            index++;
        }
        resultHolder = objectPool.allocate(finfo.getReturnType());
    }

    @Override
    public IJObject getArgument(int index) {
        return arguments[index];
    }

    @Override
    public void setResult(IJObject result) throws IOException, AsterixException {
        IAObject obj = result.getIAObject();
        try {
            outputProvider.getDataOutput().writeByte(obj.getType().getTypeTag().serialize());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

        if (obj.getType().getTypeTag().equals(ATypeTag.RECORD)) {
            ARecordType recType = (ARecordType) obj.getType();
            if (recType.isOpen()) {
                writeOpenRecord((JRecord) result, outputProvider.getDataOutput());
            } else {
                resultSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(recType);
                resultSerde.serialize(obj, outputProvider.getDataOutput());
            }
        } else {
            resultSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(obj.getType());
            resultSerde.serialize(obj, outputProvider.getDataOutput());
        }
        reset();
    }

    private void writeOpenRecord(JRecord jRecord, DataOutput dataOutput) throws AsterixException, IOException {
        ARecord aRecord = (ARecord) jRecord.getIAObject();
        RecordBuilder recordBuilder = new RecordBuilder();
        ARecordType recordType = aRecord.getType();
        recordBuilder.reset(recordType);
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        List<Boolean> openField = jRecord.getOpenField();

        int fieldIndex = 0;
        int closedFieldId = 0;
        for (IJObject field : jRecord.getFields()) {
            fieldValue.reset();
            switch (field.getTypeTag()) {
                case RECORD:
                    ARecordType recType = (ARecordType) field.getIAObject().getType();
                    if (recType.isOpen()) {
                        fieldValue.getDataOutput().writeByte(recType.getTypeTag().serialize());
                        writeOpenRecord((JRecord) field, fieldValue.getDataOutput());
                    } else {
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(
                                field.getIAObject().getType()).serialize(field.getIAObject(),
                                fieldValue.getDataOutput());
                    }
                    break;
                default:
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(field.getIAObject().getType())
                            .serialize(field.getIAObject(), fieldValue.getDataOutput());
                    break;
            }
            if (openField.get(fieldIndex)) {
                String fName = jRecord.getFieldNames().get(fieldIndex);
                fieldName.reset();
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING).serialize(
                        new AString(fName), fieldName.getDataOutput());
                recordBuilder.addField(fieldName, fieldValue);
            } else {
                recordBuilder.addField(closedFieldId, fieldValue);
                closedFieldId++;
            }
            fieldIndex++;
        }

        recordBuilder.write(dataOutput, false);

    }

    private void reset() {
        for (IJObject jObject : arguments) {
            switch (jObject.getTypeTag()) {
                case RECORD:
                    reset((JRecord) jObject);
                    break;
            }
        }
        switch (resultHolder.getTypeTag()) {
            case RECORD:
                reset((JRecord) resultHolder);
                break;
        }
    }

    private void reset(JRecord jRecord) {
        List<IJObject> fields = ((JRecord) jRecord).getFields();
        for (IJObject field : fields) {
            switch (field.getTypeTag()) {
                case RECORD:
                    reset((JRecord) field);
                    break;
            }
        }
        jRecord.close();
    }

    public void setArgument(int index, byte[] argument) throws IOException, AsterixException {
        bis.setContent(argument, 1, argument.length);
        IAType type = finfo.getParamList().get(index);
        arguments[index] = JObjectUtil.getJType(type.getTypeTag(), type, dis, objectPool);
    }

    @Override
    public IJObject getResultObject() {
        return resultHolder;
    }

    @Override
    public IJObject getObject(JTypeTag jtypeTag) {
        IJObject retValue = null;
        switch (jtypeTag) {
            case INT:
                retValue = objectPool.allocate(BuiltinType.AINT32);
                break;
            case STRING:
                retValue = objectPool.allocate(BuiltinType.ASTRING);
                break;
        }
        return retValue;
    }

}