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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractSerializableAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import edu.uci.ics.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableGlobalAvgAggregateDescriptor extends AbstractSerializableAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SerializableGlobalAvgAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SERIAL_GLOBAL_AVG;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            ICopyEvaluatorFactory[] args) throws AlgebricksException {
        final ICopyEvaluatorFactory[] evals = args;
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        unionList.add(BuiltinType.ADOUBLE);
        ARecordType _recType;
        try {
            _recType = new ARecordType(null, new String[] { "sum", "count" }, new IAType[] {
                    new AUnionType(unionList, "OptionalDouble"), BuiltinType.AINT64 }, true);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }

        final ARecordType recType = _recType;

        return new ICopySerializableAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopySerializableAggregateFunction createAggregateFunction() throws AlgebricksException {

                return new ICopySerializableAggregateFunction() {
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = evals[0].createEvaluator(inputVal);
                    private AMutableDouble aDouble = new AMutableDouble(0);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);

                    private ArrayBackedValueStorage avgBytes = new ArrayBackedValueStorage();
                    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
                    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
                    private ICopyEvaluator evalSum = new AccessibleByteArrayEval(avgBytes.getDataOutput(), sumBytes);
                    private ICopyEvaluator evalCount = new AccessibleByteArrayEval(avgBytes.getDataOutput(), countBytes);
                    private ClosedRecordConstructorEval recordEval;

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt64> longSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
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
                        double globalSum = BufferSerDeUtil.getDouble(state, start);
                        long globalCount = BufferSerDeUtil.getLong(state, start + 8);
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start + 16);

                        inputVal.reset();
                        eval.evaluate(tuple);
                        byte[] serBytes = inputVal.getByteArray();
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[0]);
                        switch (typeTag) {
                            case NULL: {
                                metNull = true;
                                break;
                            }
                            case SYSTEM_NULL: {
                                // Ignore and return.
                                return;
                            }
                            case RECORD: {
                                // Expected.
                                break;
                            }
                            default: {
                                throw new AlgebricksException("Global-Avg is not defined for values of type "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[0]));
                            }
                        }
                        int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, 0, 1, true);
                        if (offset1 == 0) // the sum is null
                            metNull = true;
                        else
                            globalSum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                        int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, 1, 1, true);
                        if (offset2 != 0) // the count is not null
                            globalCount += AInt64SerializerDeserializer.getLong(serBytes, offset2);

                        BufferSerDeUtil.writeDouble(globalSum, state, start);
                        BufferSerDeUtil.writeLong(globalCount, state, start + 8);
                        BufferSerDeUtil.writeBoolean(metNull, state, start + 16);

                    }

                    @Override
                    public void finish(byte[] state, int start, int len, DataOutput result) throws AlgebricksException {
                        double globalSum = BufferSerDeUtil.getDouble(state, start);
                        long globalCount = BufferSerDeUtil.getLong(state, start + 8);
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start + 16);

                        try {
                            if (globalCount == 0 || metNull)
                                nullSerde.serialize(ANull.NULL, result);
                            else {
                                aDouble.setValue(globalSum / globalCount);
                                doubleSerde.serialize(aDouble, result);
                            }
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public void finishPartial(byte[] state, int start, int len, DataOutput result)
                            throws AlgebricksException {
                        double globalSum = BufferSerDeUtil.getDouble(state, start);
                        long globalCount = BufferSerDeUtil.getLong(state, start + 8);
                        boolean metNull = BufferSerDeUtil.getBoolean(state, start + 16);

                        if (recordEval == null)
                            recordEval = new ClosedRecordConstructorEval(recType, new ICopyEvaluator[] { evalSum,
                                    evalCount }, avgBytes, result);

                        try {
                            if (globalCount == 0 || metNull) {
                                sumBytes.reset();
                                nullSerde.serialize(ANull.NULL, sumBytesOutput);
                            } else {
                                sumBytes.reset();
                                aDouble.setValue(globalSum);
                                doubleSerde.serialize(aDouble, sumBytesOutput);
                            }
                            countBytes.reset();
                            aInt64.setValue(globalCount);
                            longSerde.serialize(aInt64, countBytesOutput);
                            recordEval.evaluate(null);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

}
