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

import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AIntervalConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.INTERVAL_CONSTRUCTOR;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AIntervalConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(argOut0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(argOut1);
                    private String errorMessage = "This can not be an instance of interval (only support Date/Time/Datetime)";
                    private AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINTERVAL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        argOut0.reset();
                        argOut1.reset();
                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);

                        try {

                            if (argOut0.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || argOut1.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            if (argOut0.getByteArray()[0] != argOut1.getByteArray()[0]) {
                                throw new AlgebricksException(
                                        FID.getName()
                                                + ": expects both arguments to be of the same type. Either DATE/TIME/DATETIME, but got "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(
                                                        argOut0.getByteArray()[0])
                                                + " and " + EnumDeserializer.ATYPETAGDESERIALIZER
                                                        .deserialize(argOut0.getByteArray()[1]));
                            }

                            long intervalStart = 0, intervalEnd = 0;
                            ATypeTag intervalType = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(argOut0.getByteArray()[0]);

                            switch (intervalType) {
                                case DATE:
                                    intervalStart = ADateSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    intervalEnd = ADateSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                    break;
                                case TIME:
                                    intervalStart = ATimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    intervalEnd = ATimeSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                    break;
                                case DATETIME:
                                    intervalStart = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(),
                                            1);
                                    intervalEnd = ADateTimeSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                    break;
                                default:
                                    throw new AlgebricksException(
                                            FID.getName() + ": expects NULL/DATE/TIME/DATETIME as arguments, but got "
                                                    + intervalType);
                            }

                            if (intervalEnd < intervalStart) {
                                throw new AlgebricksException(
                                        FID.getName() + ": interval end must not be less than the interval start.");
                            }

                            aInterval.setValue(intervalStart, intervalEnd, intervalType.serialize());
                            intervalSerde.serialize(aInterval, out);

                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        } catch (Exception e2) {
                            throw new AlgebricksException(e2);
                        }
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