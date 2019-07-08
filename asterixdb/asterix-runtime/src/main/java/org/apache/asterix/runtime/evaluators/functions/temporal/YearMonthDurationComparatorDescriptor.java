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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class YearMonthDurationComparatorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final FunctionIdentifier GREATER_THAN_FID = BuiltinFunctions.YEAR_MONTH_DURATION_GREATER_THAN;
    public static final FunctionIdentifier LESS_THAN_FID = BuiltinFunctions.YEAR_MONTH_DURATION_LESS_THAN;
    private final boolean isGreaterThan;

    protected YearMonthDurationComparatorDescriptor(boolean isGreaterThan) {
        this.isGreaterThan = isGreaterThan;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> boolSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();

                        if (bytes0[offset0] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_DURATION_TYPE_TAG);
                        }
                        if (bytes1[offset1] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_DURATION_TYPE_TAG);
                        }

                        if ((ADurationSerializerDeserializer.getDayTime(bytes0, offset0 + 1) != 0)
                                || (ADurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1) != 0)) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG);
                        }

                        if (ADurationSerializerDeserializer.getYearMonth(bytes0,
                                offset0 + 1) > ADurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1)) {
                            boolSerde.serialize(isGreaterThan ? ABoolean.TRUE : ABoolean.FALSE, out);
                        } else {
                            boolSerde.serialize(isGreaterThan ? ABoolean.FALSE : ABoolean.TRUE, out);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.AbstractFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return isGreaterThan ? GREATER_THAN_FID : LESS_THAN_FID;
    }

}
