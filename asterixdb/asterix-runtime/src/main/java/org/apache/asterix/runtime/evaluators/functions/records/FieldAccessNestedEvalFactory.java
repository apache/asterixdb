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
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessNestedEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory recordEvalFactory;
    private final ARecordType recordType;
    private final List<String> fieldPath;
    private final SourceLocation sourceLoc;

    public FieldAccessNestedEvalFactory(IScalarEvaluatorFactory recordEvalFactory, ARecordType recordType,
            List<String> fldName, SourceLocation sourceLoc) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
        this.fieldPath = fldName;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {

            private final IBinaryHashFunction fieldNameHashFunction =
                    BinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryHashFunction();
            private final IBinaryComparator fieldNameComparator =
                    BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
            private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private final DataOutput out = resultStorage.getDataOutput();
            private final ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private final IPointable inputArg0 = new VoidPointable();
            private final IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private final IPointable[] fieldPointables = new VoidPointable[fieldPath.size()];
            private final RuntimeRecordTypeInfo[] recTypeInfos = new RuntimeRecordTypeInfo[fieldPath.size()];
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<AMissing> missingSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);

            {
                generateFieldsPointables();
                for (int index = 0; index < fieldPath.size(); ++index) {
                    recTypeInfos[index] = new RuntimeRecordTypeInfo();
                }

            }

            @SuppressWarnings("unchecked")
            private void generateFieldsPointables() throws HyracksDataException {
                for (int i = 0; i < fieldPath.size(); i++) {
                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    DataOutput out = storage.getDataOutput();
                    AString as = new AString(fieldPath.get(i));
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, out);
                    fieldPointables[i] = new VoidPointable();
                    fieldPointables[i].set(storage);
                }
            }

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                try {
                    resultStorage.reset();
                    eval0.evaluate(tuple, inputArg0);

                    if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0)) {
                        return;
                    }

                    byte[] serRecord = inputArg0.getByteArray();
                    int offset = inputArg0.getStartOffset();
                    int start = offset;
                    int len = inputArg0.getLength();

                    if (serRecord[start] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        throw new TypeMismatchException(sourceLoc, serRecord[start],
                                ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                    }

                    int subFieldIndex = -1;
                    int subFieldOffset = -1;
                    int subFieldLength = -1;
                    int nullBitmapSize = -1;

                    IAType subType = recordType;
                    recTypeInfos[0].reset(recordType);

                    ATypeTag subTypeTag = ATypeTag.MISSING;
                    boolean openField = false;
                    int pathIndex = 0;

                    // Moving through closed fields first.
                    for (; pathIndex < fieldPointables.length; pathIndex++) {
                        if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                            //enforced SubType
                            subType = ((AUnionType) subType).getActualType();
                            byte serializedTypeTag = subType.getTypeTag().serialize();
                            if (serializedTypeTag != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, serializedTypeTag,
                                        ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                            }
                            if (subType.getTypeTag() == ATypeTag.OBJECT) {
                                recTypeInfos[pathIndex].reset((ARecordType) subType);
                            }
                        }
                        subFieldIndex = recTypeInfos[pathIndex].getFieldIndex(fieldPointables[pathIndex].getByteArray(),
                                fieldPointables[pathIndex].getStartOffset() + 1,
                                fieldPointables[pathIndex].getLength() - 1);
                        if (subFieldIndex == -1) {
                            break;
                        }
                        nullBitmapSize = RecordUtil.computeNullBitmapSize((ARecordType) subType);
                        subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, start,
                                subFieldIndex, nullBitmapSize, ((ARecordType) subType).isOpen());
                        if (subFieldOffset == 0) {
                            // the field is null, we checked the null bit map
                            // any path after null will return null.
                            nullSerde.serialize(ANull.NULL, out);
                            result.set(resultStorage);
                            return;
                        }
                        if (subFieldOffset < 0) {
                            // the field is missing, we checked the missing bit map
                            // any path after missing will return null.
                            missingSerde.serialize(AMissing.MISSING, out);
                            result.set(resultStorage);
                            return;
                        }
                        subType = ((ARecordType) subType).getFieldTypes()[subFieldIndex];
                        if (subType.getTypeTag() == ATypeTag.OBJECT && pathIndex + 1 < fieldPointables.length) {
                            // Move to the next Depth
                            recTypeInfos[pathIndex + 1].reset((ARecordType) subType);
                        }
                        if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                            subTypeTag = ((AUnionType) subType).getActualType().getTypeTag();
                            subFieldLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset,
                                    subTypeTag, false);
                        } else {
                            subTypeTag = subType.getTypeTag();
                            subFieldLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset,
                                    subTypeTag, false);
                        }

                        if (pathIndex < fieldPointables.length - 1) {
                            //setup next iteration
                            subRecordTmpStream.reset();
                            subRecordTmpStream.write(subTypeTag.serialize());
                            subRecordTmpStream.write(serRecord, subFieldOffset, subFieldLength);
                            serRecord = subRecordTmpStream.getByteArray();
                            start = 0;
                        }
                        // type check
                        if (pathIndex < fieldPointables.length - 1
                                && serRecord[start] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, serRecord[start],
                                    ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                        }
                    }

                    // Moving through open fields after we hit the first open field.
                    for (; pathIndex < fieldPointables.length; pathIndex++) {
                        openField = true;
                        subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetByName(serRecord, start, len,
                                fieldPointables[pathIndex].getByteArray(), fieldPointables[pathIndex].getStartOffset(),
                                fieldNameHashFunction, fieldNameComparator);
                        if (subFieldOffset < 0) {
                            out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                            result.set(resultStorage);
                            return;
                        }

                        subTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[subFieldOffset]);
                        subFieldLength =
                                NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset, subTypeTag, true)
                                        + 1;

                        if (pathIndex >= fieldPointables.length - 1) {
                            continue;
                        }
                        //setup next iteration
                        start = subFieldOffset;
                        len = subFieldLength;

                        // type check
                        if (serRecord[start] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                            missingSerde.serialize(AMissing.MISSING, out);
                            result.set(resultStorage);
                            return;
                        }
                        if (serRecord[start] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, serRecord[start],
                                    ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                        }
                    }
                    // emit the final result.
                    if (openField) {
                        result.set(serRecord, subFieldOffset, subFieldLength);
                    } else {
                        out.writeByte(subTypeTag.serialize());
                        out.write(serRecord, subFieldOffset, subFieldLength);
                        result.set(resultStorage);
                    }
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
