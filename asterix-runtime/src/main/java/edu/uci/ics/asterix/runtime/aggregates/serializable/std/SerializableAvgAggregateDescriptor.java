/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractSerializableAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableAvgAggregateDescriptor extends AbstractSerializableAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SerializableAvgAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SERIAL_AVG;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            ICopyEvaluatorFactory[] args) throws AlgebricksException {
        final ICopyEvaluatorFactory[] evals = args;

        return new ICopySerializableAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            public ICopySerializableAggregateFunction createAggregateFunction() throws AlgebricksException {
                return new ICopySerializableAggregateFunction() {
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = evals[0].createEvaluator(inputVal);

                    private AMutableDouble aDouble = new AMutableDouble(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void init(DataOutput state) throws AlgebricksException {
                        try {
                            state.writeDouble(0.0);
                            state.writeLong(0);
                            state.writeBoolean(false);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public void step(IFrameTupleReference tuple, byte[] state, int start, int len)
                            throws AlgebricksException {
                        inputVal.reset();
                        eval.evaluate(tuple);
                        double sum = BufferSerDeUtil.getDouble(state, start);
                        long count = BufferSerDeUtil.getLong(state, start + 8);
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start + 16);
                        if (inputVal.getLength() > 0) {
                            ++count;
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal
                                    .getByteArray()[0]);
                            switch (typeTag) {
                                case INT8: {
                                    byte val = AInt8SerializerDeserializer.getByte(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT16: {
                                    short val = AInt16SerializerDeserializer.getShort(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT32: {
                                    int val = AInt32SerializerDeserializer.getInt(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT64: {
                                    long val = AInt64SerializerDeserializer.getLong(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case FLOAT: {
                                    float val = AFloatSerializerDeserializer.getFloat(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case DOUBLE: {
                                    double val = ADoubleSerializerDeserializer.getDouble(inputVal.getByteArray(), 1);
                                    sum += val;
                                    break;
                                }
                                case NULL: {
                                    metNull = true;
                                    break;
                                }
                                default: {
                                    throw new NotImplementedException("Cannot compute AVG for values of type "
                                            + typeTag);
                                }
                            }
                            inputVal.reset();
                        }
                        BufferSerDeUtil.writeDouble(sum, state, start);
                        BufferSerDeUtil.writeLong(count, state, start + 8);
                        BufferSerDeUtil.writeBoolean(metNull, state, start + 16);
                    }

                    @Override
                    public void finish(byte[] state, int start, int len, DataOutput result) throws AlgebricksException {
                        double sum = BufferSerDeUtil.getDouble(state, start);
                        long count = BufferSerDeUtil.getLong(state, start + 8);
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start + 16);

                        if (count == 0) {
                            GlobalConfig.ASTERIX_LOGGER.fine("AVG aggregate ran over empty input.");
                        } else {
                            try {
                                if (metNull)
                                    nullSerde.serialize(ANull.NULL, result);
                                else {
                                    aDouble.setValue(sum / count);
                                    doubleSerde.serialize(aDouble, result);
                                }
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            }
                        }
                    }

                    @Override
                    public void finishPartial(byte[] data, int start, int len, DataOutput partialResult)
                            throws AlgebricksException {
                        try {
                            partialResult.write(data, start, len);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                };
            }
        };
    }
}
