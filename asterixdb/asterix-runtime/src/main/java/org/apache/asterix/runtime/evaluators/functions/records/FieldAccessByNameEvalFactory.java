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
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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

public class FieldAccessByNameEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory recordEvalFactory;
    private final IScalarEvaluatorFactory fldNameEvalFactory;
    private final SourceLocation sourceLoc;
    private final FunctionIdentifier funID;

    public FieldAccessByNameEvalFactory(IScalarEvaluatorFactory recordEvalFactory,
            IScalarEvaluatorFactory fldNameEvalFactory, SourceLocation sourceLoc, FunctionIdentifier funID) {
        this.recordEvalFactory = recordEvalFactory;
        this.fldNameEvalFactory = fldNameEvalFactory;
        this.sourceLoc = sourceLoc;
        this.funID = funID;
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
            private final IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private final IScalarEvaluator eval1 = fldNameEvalFactory.createScalarEvaluator(ctx);

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
                    int serRecordOffset = inputArg0.getStartOffset();
                    int serRecordLen = inputArg0.getLength();

                    if (serRecord[serRecordOffset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funID, serRecord[serRecordOffset], 0,
                                ATypeTag.OBJECT);
                        out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }
                    byte[] serFldName = inputArg1.getByteArray();
                    int serFldNameOffset = inputArg1.getStartOffset();
                    if (serFldName[serFldNameOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                        ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funID, serFldName[serFldNameOffset], 1,
                                ATypeTag.STRING);
                        out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }
                    int fieldValueOffset =
                            ARecordSerializerDeserializer.getFieldOffsetByName(serRecord, serRecordOffset, serRecordLen,
                                    serFldName, serFldNameOffset, fieldNameHashFunction, fieldNameComparator);
                    if (fieldValueOffset < 0) {
                        out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                        result.set(resultStorage);
                        return;
                    }

                    ATypeTag fieldValueTypeTag =
                            EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[fieldValueOffset]);
                    int fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                            fieldValueTypeTag, true) + 1;
                    result.set(serRecord, fieldValueOffset, fieldValueLength);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
