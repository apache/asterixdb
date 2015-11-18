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
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessUtil {

    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    @SuppressWarnings("unchecked")
    public static void getFieldsAbvs(ArrayBackedValueStorage[] abvsFields, DataOutput[] doFields,
            List<String> fieldPaths) throws AlgebricksException {
        AString as;
        for (int i = 0; i < fieldPaths.size(); i++) {
            abvsFields[i] = new ArrayBackedValueStorage();
            doFields[i] = abvsFields[i].getDataOutput();
            as = new AString(fieldPaths.get(i));
            try {
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                        doFields[i]);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        }
    }

    public static boolean checkType(byte tagId, DataOutput out) throws AlgebricksException {
        if (tagId == SER_NULL_TYPE_TAG) {
            try {
                nullSerde.serialize(ANull.NULL, out);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            return true;
        }

        if (tagId != SER_RECORD_TYPE_TAG) {
            throw new AlgebricksException("Field accessor is not defined for values of type "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tagId));
        }
        return false;
    }

    public static void evaluate(IFrameTupleReference tuple, DataOutput out, ICopyEvaluator eval0,
            ArrayBackedValueStorage[] abvsFields, ArrayBackedValueStorage abvsRecord,
            ByteArrayAccessibleOutputStream subRecordTmpStream, ARecordType recordType,
            RuntimeRecordTypeInfo[] recTypeInfos) throws AlgebricksException {
        try {
            abvsRecord.reset();
            eval0.evaluate(tuple);

            int subFieldIndex = -1;
            int subFieldOffset = -1;
            int subFieldLength = -1;
            int nullBitmapSize = -1;

            IAType subType = recordType;
            recTypeInfos[0].reset(recordType);

            ATypeTag subTypeTag = ATypeTag.NULL;
            byte[] subRecord = abvsRecord.getByteArray();
            boolean openField = false;
            int i = 0;

            if (checkType(subRecord[0], out)) {
                return;
            }

            //Moving through closed fields
            for (; i < abvsFields.length; i++) {
                if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                    //enforced SubType
                    subType = ((AUnionType) subType).getNullableType();
                    if (subType.getTypeTag().serialize() != SER_RECORD_TYPE_TAG) {
                        throw new AlgebricksException("Field accessor is not defined for values of type " + subTypeTag);
                    }
                    if (subType.getTypeTag() == ATypeTag.RECORD) {
                        recTypeInfos[i].reset((ARecordType) subType);
                    }
                }
                subFieldIndex = recTypeInfos[i].getFieldIndex(abvsFields[i].getByteArray(),
                        abvsFields[i].getStartOffset() + 1, abvsFields[i].getLength());
                if (subFieldIndex == -1) {
                    break;
                }
                nullBitmapSize = ARecordType.computeNullBitmapSize((ARecordType) subType);
                subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetById(subRecord, subFieldIndex,
                        nullBitmapSize, ((ARecordType) subType).isOpen());
                if (subFieldOffset == 0) {
                    // the field is null, we checked the null bit map
                    out.writeByte(SER_NULL_TYPE_TAG);
                    return;
                }
                subType = ((ARecordType) subType).getFieldTypes()[subFieldIndex];
                if (subType.getTypeTag() == ATypeTag.RECORD && i + 1 < abvsFields.length) {
                    // Move to the next Depth
                    recTypeInfos[i + 1].reset((ARecordType) subType);
                }
                if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                    if (((AUnionType) subType).isNullableType()) {
                        subTypeTag = ((AUnionType) subType).getNullableType().getTypeTag();
                        subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset, subTypeTag,
                                false);
                    } else {
                        // union .. the general case
                        throw new NotImplementedException();
                    }
                } else {
                    subTypeTag = subType.getTypeTag();
                    subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset, subTypeTag,
                            false);
                }

                if (i < abvsFields.length - 1) {
                    //setup next iteration
                    subRecordTmpStream.reset();
                    subRecordTmpStream.write(subTypeTag.serialize());
                    subRecordTmpStream.write(subRecord, subFieldOffset, subFieldLength);
                    subRecord = subRecordTmpStream.getByteArray();

                    if (checkType(subRecord[0], out)) {
                        return;
                    }
                }
            }

            //Moving through open fields
            for (; i < abvsFields.length; i++) {
                openField = true;
                subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetByName(subRecord,
                        abvsFields[i].getByteArray());
                if (subFieldOffset < 0) {
                    out.writeByte(SER_NULL_TYPE_TAG);
                    return;
                }

                subTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(subRecord[subFieldOffset]);
                subFieldLength = NonTaggedFormatUtil.getFieldValueLength(subRecord, subFieldOffset, subTypeTag, true)
                        + 1;

                if (i < abvsFields.length - 1) {
                    //setup next iteration
                    subRecord = Arrays.copyOfRange(subRecord, subFieldOffset, subFieldOffset + subFieldLength);

                    if (checkType(subRecord[0], out)) {
                        return;
                    }
                }
            }
            if (!openField) {
                out.writeByte(subTypeTag.serialize());
            }
            out.write(subRecord, subFieldOffset, subFieldLength);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
    }
}
