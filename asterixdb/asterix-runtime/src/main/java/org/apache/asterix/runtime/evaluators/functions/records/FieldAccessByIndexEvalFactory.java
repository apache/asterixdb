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
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessByIndexEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory recordEvalFactory;
    private final IScalarEvaluatorFactory fieldIndexEvalFactory;
    private final int nullBitmapSize;
    private final ARecordType recordType;
    private final SourceLocation sourceLoc;

    FieldAccessByIndexEvalFactory(IScalarEvaluatorFactory recordEvalFactory,
            IScalarEvaluatorFactory fieldIndexEvalFactory, ARecordType recordType, SourceLocation sourceLoc) {
        this.recordEvalFactory = recordEvalFactory;
        this.fieldIndexEvalFactory = fieldIndexEvalFactory;
        this.recordType = recordType;
        this.nullBitmapSize = recordType != null ? RecordUtil.computeNullBitmapSize(recordType) : 0;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {
            private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private final DataOutput out = resultStorage.getDataOutput();
            private final IPointable inputArg0 = new VoidPointable();
            private final IPointable inputArg1 = new VoidPointable();
            private final IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private final IScalarEvaluator eval1 = fieldIndexEvalFactory.createScalarEvaluator(ctx);
            private int fieldIndex;
            private int fieldValueOffset;
            private int fieldValueLength;
            private IAType fieldValueType;
            private ATypeTag fieldValueTypeTag;

            // inputArg0: the record, inputArg1: the index
            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                try {
                    resultStorage.reset();
                    eval0.evaluate(tuple, inputArg0);
                    eval1.evaluate(tuple, inputArg1);

                    if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                        return;
                    }

                    byte[] serRecord = inputArg0.getByteArray();
                    int offset = inputArg0.getStartOffset();
                    byte[] indexBytes = inputArg1.getByteArray();
                    int indexOffset = inputArg1.getStartOffset();

                    if (serRecord[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG || recordType == null) {
                        // recordType = null should only mean first arg was not a record and compiler couldn't set it
                        throw new TypeMismatchException(sourceLoc, serRecord[offset],
                                ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                    }

                    if (indexBytes[indexOffset] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                        throw new TypeMismatchException(sourceLoc, indexBytes[offset],
                                ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                    }
                    fieldIndex = IntegerPointable.getInteger(indexBytes, indexOffset + 1);
                    fieldValueType = recordType.getFieldTypes()[fieldIndex];
                    fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetById(serRecord, offset, fieldIndex,
                            nullBitmapSize, recordType.isOpen());

                    if (fieldValueOffset == 0) {
                        // the field is null, we checked the null bit map
                        out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }
                    if (fieldValueOffset < 0) {
                        // the field is missing, we checked the missing bit map
                        out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }

                    if (fieldValueType.getTypeTag().equals(ATypeTag.UNION)) {
                        if (((AUnionType) fieldValueType).isUnknownableType()) {
                            fieldValueTypeTag = ((AUnionType) fieldValueType).getActualType().getTypeTag();
                            fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                                    fieldValueTypeTag, false);
                            out.writeByte(fieldValueTypeTag.serialize());
                        } else {
                            // union .. the general case
                            throw new NotImplementedException();
                        }
                    } else {
                        fieldValueTypeTag = fieldValueType.getTypeTag();
                        fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                                fieldValueTypeTag, false);
                        out.writeByte(fieldValueTypeTag.serialize());
                    }
                    out.write(serRecord, fieldValueOffset, fieldValueLength);
                    result.set(resultStorage);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
