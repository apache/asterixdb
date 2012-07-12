package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import edu.uci.ics.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GlobalAvgAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-avg",
            1);
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new GlobalAvgAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        unionList.add(BuiltinType.ADOUBLE);
        final ARecordType recType = new ARecordType(null, new String[] { "sum", "count" }, new IAType[] {
                new AUnionType(unionList, "OptionalDouble"), BuiltinType.AINT32 }, false);

        return new ICopyAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyAggregateFunction createAggregateFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {

                return new ICopyAggregateFunction() {

                    private DataOutput out = provider.getDataOutput();
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(inputVal);
                    private double globalSum;
                    private int globalCount;
                    private AMutableDouble aDouble = new AMutableDouble(0);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);

                    private ArrayBackedValueStorage avgBytes = new ArrayBackedValueStorage();
                    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
                    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
                    private ICopyEvaluator evalSum = new AccessibleByteArrayEval(avgBytes.getDataOutput(), sumBytes);
                    private ICopyEvaluator evalCount = new AccessibleByteArrayEval(avgBytes.getDataOutput(), countBytes);
                    private ClosedRecordConstructorEval recordEval = new ClosedRecordConstructorEval(recType,
                            new ICopyEvaluator[] { evalSum, evalCount }, avgBytes, out);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private boolean metNull;

                    @Override
                    public void init() {
                        globalSum = 0.0;
                        globalCount = 0;
                        metNull = false;
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        eval.evaluate(tuple);
                        byte[] serBytes = inputVal.getByteArray();
                        if (serBytes[0] == SER_NULL_TYPE_TAG)
                            metNull = true;
                        if (serBytes[0] != SER_RECORD_TYPE_TAG) {
                            throw new AlgebricksException("Global-Avg is not defined for values of type "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[0]));
                        }
                        int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, 0, 1, false);
                        if (offset1 == 0) // the sum is null
                            metNull = true;
                        else
                            globalSum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                        int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, 1, 1, false);
                        if (offset2 != 0) // the count is not null
                            globalCount += AInt32SerializerDeserializer.getInt(serBytes, offset2);

                    }

                    @Override
                    public void finish() throws AlgebricksException {
                        if (globalCount == 0) {
                            GlobalConfig.ASTERIX_LOGGER.fine("AVG aggregate ran over empty input.");
                        } else {
                            try {
                                if (metNull)
                                    nullSerde.serialize(ANull.NULL, out);
                                else {
                                    aDouble.setValue(globalSum / globalCount);
                                    doubleSerde.serialize(aDouble, out);
                                }
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            }
                        }
                    }

                    @Override
                    public void finishPartial() throws AlgebricksException {
                        if (globalCount == 0) {
                            GlobalConfig.ASTERIX_LOGGER.fine("AVG aggregate ran over empty input.");
                        } else {
                            try {
                                if (metNull) {
                                    sumBytes.reset();
                                    nullSerde.serialize(ANull.NULL, sumBytesOutput);
                                } else {
                                    sumBytes.reset();
                                    aDouble.setValue(globalSum);
                                    doubleSerde.serialize(aDouble, sumBytesOutput);
                                }
                                countBytes.reset();
                                aInt32.setValue(globalCount);
                                intSerde.serialize(aInt32, countBytesOutput);
                                recordEval.evaluate(null);
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            }
                        }
                    }
                };
            }
        };
    }

}
