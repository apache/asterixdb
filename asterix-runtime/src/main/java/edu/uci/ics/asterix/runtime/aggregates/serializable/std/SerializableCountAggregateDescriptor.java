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

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractSerializableAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * count(NULL) returns NULL.
 */
public class SerializableCountAggregateDescriptor extends AbstractSerializableAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SerializableCountAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SERIAL_COUNT;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopySerializableAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopySerializableAggregateFunction createAggregateFunction() throws AlgebricksException {

                return new ICopySerializableAggregateFunction() {
                    private AMutableInt64 result = new AMutableInt64(-1);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(inputVal);

                    @Override
                    public void init(DataOutput state) throws AlgebricksException {
                        try {
                            state.writeBoolean(false);
                            state.writeLong(0);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public void step(IFrameTupleReference tuple, byte[] state, int start, int len)
                            throws AlgebricksException {
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start);
                        long cnt = BufferSerDeUtil.getLong(state, start + 1);
                        inputVal.reset();
                        eval.evaluate(tuple);
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(inputVal.getByteArray()[0]);
                        if (typeTag == ATypeTag.NULL) {
                            metNull = true;
                        } else {
                            cnt++;
                        }
                        BufferSerDeUtil.writeBoolean(metNull, state, start);
                        BufferSerDeUtil.writeLong(cnt, state, start + 1);
                    }

                    @Override
                    public void finish(byte[] state, int start, int len, DataOutput out) throws AlgebricksException {
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start);
                        long cnt = BufferSerDeUtil.getLong(state, start + 1);
                        try {
                            if (metNull) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                result.setValue(cnt);
                                int64Serde.serialize(result, out);
                            }
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public void finishPartial(byte[] state, int start, int len, DataOutput out)
                            throws AlgebricksException {
                        finish(state, start, len, out);
                    }
                };
            }
        };
    }

}
