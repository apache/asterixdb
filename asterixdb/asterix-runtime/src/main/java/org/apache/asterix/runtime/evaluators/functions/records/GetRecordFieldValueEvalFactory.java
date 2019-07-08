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

import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldValueEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory recordEvalFactory;
    private IScalarEvaluatorFactory fldNameEvalFactory;
    private final ARecordType recordType;
    private final SourceLocation sourceLoc;

    public GetRecordFieldValueEvalFactory(IScalarEvaluatorFactory recordEvalFactory,
            IScalarEvaluatorFactory fldNameEvalFactory, ARecordType recordType, SourceLocation sourceLoc) {
        this.recordEvalFactory = recordEvalFactory;
        this.fldNameEvalFactory = fldNameEvalFactory;
        this.recordType = recordType;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {

            private final IBinaryHashFunction fieldNameHashFunction =
                    BinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryHashFunction();
            private final IBinaryComparator fieldNameComparator =
                    BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
            private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private final DataOutput out = resultStorage.getDataOutput();

            private final IPointable inputArg0 = new VoidPointable();
            private final IPointable inputArg1 = new VoidPointable();
            private final IScalarEvaluator recordEval = recordEvalFactory.createScalarEvaluator(ctx);
            private final IScalarEvaluator fieldNameEval = fldNameEvalFactory.createScalarEvaluator(ctx);
            private final RuntimeRecordTypeInfo recTypeInfo = new RuntimeRecordTypeInfo();

            {
                recTypeInfo.reset(recordType);
            }

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                try {
                    resultStorage.reset();
                    recordEval.evaluate(tuple, inputArg0);
                    fieldNameEval.evaluate(tuple, inputArg1);

                    if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                        return;
                    }

                    byte[] serFldName = inputArg1.getByteArray();
                    int serFldNameOffset = inputArg1.getStartOffset();
                    int serFldNameLen = inputArg1.getLength();

                    byte[] serRecord = inputArg0.getByteArray();
                    int serRecordOffset = inputArg0.getStartOffset();
                    int serRecordLen = inputArg0.getLength();

                    if (serRecord[serRecordOffset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        throw new TypeMismatchException(sourceLoc, BuiltinFunctions.GET_RECORD_FIELD_VALUE, 0,
                                serRecord[serRecordOffset], ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                    }

                    int subFieldOffset = -1;
                    int subFieldLength = -1;

                    // Look at closed fields first.
                    int subFieldIndex = recTypeInfo.getFieldIndex(serFldName, serFldNameOffset + 1, serFldNameLen - 1);
                    if (subFieldIndex >= 0) {
                        int nullBitmapSize = RecordUtil.computeNullBitmapSize(recordType);
                        subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, serRecordOffset,
                                subFieldIndex, nullBitmapSize, recordType.isOpen());
                        if (subFieldOffset == 0) {
                            // the field is null, we checked the null bit map
                            out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                            result.set(resultStorage);
                            return;
                        }
                        ATypeTag fieldTypeTag = recordType.getFieldTypes()[subFieldIndex].getTypeTag();
                        subFieldLength =
                                NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset, fieldTypeTag, false);
                        // write result.
                        out.writeByte(fieldTypeTag.serialize());
                        out.write(serRecord, subFieldOffset, subFieldLength);
                        result.set(resultStorage);
                        return;
                    }

                    // Look at open fields.
                    subFieldOffset = ARecordSerializerDeserializer.getFieldOffsetByName(serRecord, serRecordOffset,
                            serRecordLen, serFldName, serFldNameOffset, fieldNameHashFunction, fieldNameComparator);
                    if (subFieldOffset < 0) {
                        out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }
                    // Get the field length.
                    ATypeTag fieldValueTypeTag =
                            EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[subFieldOffset]);
                    subFieldLength =
                            NonTaggedFormatUtil.getFieldValueLength(serRecord, subFieldOffset, fieldValueTypeTag, true)
                                    + 1;
                    // write result.
                    result.set(serRecord, subFieldOffset, subFieldLength);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
