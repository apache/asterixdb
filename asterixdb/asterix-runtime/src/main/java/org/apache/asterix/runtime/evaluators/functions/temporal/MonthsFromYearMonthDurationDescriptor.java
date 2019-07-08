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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
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

@MissingNullInOutFunction
public class MonthsFromYearMonthDurationDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = BuiltinFunctions.MONTHS_FROM_YEAR_MONTH_DURATION;
    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new MonthsFromYearMonthDurationDescriptor();
        }
    };

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
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt64> int64Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

                    AMutableInt64 aInt64 = new AMutableInt64(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0)) {
                            return;
                        }

                        byte[] bytes = argPtr0.getByteArray();
                        int offset = argPtr0.getStartOffset();

                        if (bytes[offset] != ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[offset],
                                    ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG);
                        }
                        aInt64.setValue(AYearMonthDurationSerializerDeserializer.getYearMonth(bytes, offset + 1));
                        int64Serde.serialize(aInt64, out);
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
        return FID;
    }

}
