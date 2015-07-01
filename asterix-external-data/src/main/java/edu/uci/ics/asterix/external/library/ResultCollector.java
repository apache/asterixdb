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
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ResultCollector implements IResultCollector {

    private IAObject reusableResultObjectHolder;
    private ByteBuffer reusableResultBinaryHolder;
    private IDataOutputProvider outputProvider;
    private IExternalFunctionInfo finfo;

    public ResultCollector(IExternalFunctionInfo finfo, IDataOutputProvider outputProvider) {
        this.finfo = finfo;
        IAType returnType = finfo.getReturnType();
        reusableResultObjectHolder = allocateResultObjectHolder(returnType);
        reusableResultBinaryHolder = allocateResultBinaryHolder(returnType);
        this.outputProvider = outputProvider;
    }

    private IAObject allocateResultObjectHolder(IAType type) {
        switch (type.getTypeTag()) {
            case INT32:
                return new AMutableInt32(0);
            case FLOAT:
                return new AMutableFloat(0f);
            case DOUBLE:
                return new AMutableDouble(0);
            case STRING:
                return new AMutableString("");
            case ORDEREDLIST:
                return new AMutableOrderedList((AOrderedListType) type);
            case RECORD:
                IAType[] fieldType = ((ARecordType) type).getFieldTypes();
                IAObject[] fieldObjects = new IAObject[fieldType.length];
                for (int i = 0; i < fieldType.length; i++) {
                    fieldObjects[i] = allocateResultObjectHolder(fieldType[i]);
                }
                return new AMutableRecord((ARecordType) type, fieldObjects);
        }
        return null;
    }

    private ByteBuffer allocateResultBinaryHolder(IAType type) {
        switch (type.getTypeTag()) {
            case INT32:
                return ByteBuffer.allocate(4);
            case FLOAT:
                return ByteBuffer.allocate(4);
            case DOUBLE:
                return ByteBuffer.allocate(8);
            case STRING:
                return ByteBuffer.allocate(32 * 1024);
            case ORDEREDLIST:
                return ByteBuffer.allocate(32 * 1024);
            case RECORD:
                return ByteBuffer.allocate(32 * 1024);
        }
        return null;
    }

    @Override
    public void writeDoubleResult(double result) throws AsterixException {
        ((AMutableDouble) reusableResultObjectHolder).setValue(result);
        serializeResult(reusableResultObjectHolder);
    }

    @Override
    public void writeFloatResult(float result) throws AsterixException {
        ((AMutableDouble) reusableResultObjectHolder).setValue(result);
        serializeResult(reusableResultObjectHolder);
    }

    @Override
    public void writeIntResult(int result) throws AsterixException {
        ((AMutableInt32) reusableResultObjectHolder).setValue(result);
        serializeResult(reusableResultObjectHolder);
    }

    @Override
    public void writeStringResult(String result) throws AsterixException {
        ((AMutableString) reusableResultObjectHolder).setValue(result);
        serializeResult(reusableResultObjectHolder);

    }

    @Override
    public void writeRecordResult(ARecord result) throws AsterixException {
        serializeResult(result);
    }

    @Override
    public void writeListResult(AOrderedList list) throws AsterixException {
        serializeResult(list);
    }

    public IAObject getComplexTypeResultHolder() {
        return reusableResultObjectHolder;
    }

    private void serializeResult(IAObject object) throws AsterixException {
        try {
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(finfo.getReturnType()).serialize(
                    reusableResultObjectHolder, outputProvider.getDataOutput());
        } catch (HyracksDataException hde) {
            throw new AsterixException(hde);
        }
    }

    @Override
    public DataOutput getDataOutput() {
        return outputProvider.getDataOutput();
    }

}
