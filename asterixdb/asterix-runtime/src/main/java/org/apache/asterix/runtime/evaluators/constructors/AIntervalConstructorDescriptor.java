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
package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
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
public class AIntervalConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = BuiltinFunctions.INTERVAL_CONSTRUCTOR;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AIntervalConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInterval> intervalSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINTERVAL);

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

                        try {
                            if (bytes0[offset0] != bytes1[offset1]) {
                                throw new IncompatibleTypeException(sourceLoc, getIdentifier(), bytes0[offset0],
                                        bytes1[offset1]);
                            }

                            long intervalStart, intervalEnd;
                            ATypeTag intervalType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);

                            switch (intervalType) {
                                case DATE:
                                    intervalStart = ADateSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    intervalEnd = ADateSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                    break;
                                case TIME:
                                    intervalStart = ATimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    intervalEnd = ATimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                    break;
                                case DATETIME:
                                    intervalStart = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    intervalEnd = ADateTimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                    break;
                                default:
                                    throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(), bytes0[offset0]);
                            }

                            if (intervalEnd < intervalStart) {
                                throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                        ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                            }

                            aInterval.setValue(intervalStart, intervalEnd, intervalType.serialize());
                            intervalSerde.serialize(aInterval, out);
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
