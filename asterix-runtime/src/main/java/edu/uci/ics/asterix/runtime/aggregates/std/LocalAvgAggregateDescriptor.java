package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
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
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class LocalAvgAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-avg",
            1, true);

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IAggregateFunctionFactory createAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException {

        return new IAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateFunction createAggregateFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {

                List<IAType> unionList = new ArrayList<IAType>();
                unionList.add(BuiltinType.ANULL);
                unionList.add(BuiltinType.ADOUBLE);
                final ARecordType recType = new ARecordType(null, new String[] { "sum", "count" }, new IAType[] {
                        new AUnionType(unionList, "OptionalDouble"), BuiltinType.AINT32 }, false);

                return new IAggregateFunction() {

                    private DataOutput out = provider.getDataOutput();
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private IEvaluator eval = args[0].createEvaluator(inputVal);
                    private double sum;
                    private int count;

                    private ArrayBackedValueStorage avgBytes = new ArrayBackedValueStorage();
                    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
                    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
                    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
                    private IEvaluator evalSum = new AccessibleByteArrayEval(avgBytes.getDataOutput(), sumBytes);
                    private IEvaluator evalCount = new AccessibleByteArrayEval(avgBytes.getDataOutput(), countBytes);
                    private ClosedRecordConstructorEval recordEval = new ClosedRecordConstructorEval(recType,
                            new IEvaluator[] { evalSum, evalCount }, avgBytes, out);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private AMutableDouble aDouble = new AMutableDouble(0);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    private boolean metNull;

                    @Override
                    public void init() {
                        sum = 0.0;
                        count = 0;
                        metNull = false;
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        eval.evaluate(tuple);
                        if (inputVal.getLength() > 0) {
                            ++count;
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(inputVal.getBytes()[0]);
                            switch (typeTag) {
                                case INT8: {
                                    byte val = AInt8SerializerDeserializer.getByte(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT16: {
                                    short val = AInt16SerializerDeserializer.getShort(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT32: {
                                    int val = AInt32SerializerDeserializer.getInt(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case INT64: {
                                    long val = AInt64SerializerDeserializer.getLong(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case FLOAT: {
                                    float val = AFloatSerializerDeserializer.getFloat(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case DOUBLE: {
                                    double val = ADoubleSerializerDeserializer.getDouble(inputVal.getBytes(), 1);
                                    sum += val;
                                    break;
                                }
                                case NULL: {
                                    metNull = true;
                                    break;
                                }
                                default: {
                                    throw new NotImplementedException("Cannot compute LOCAL-AVG for values of type "
                                            + typeTag);
                                }
                            }
                            inputVal.reset();
                        }
                    }

                    @Override
                    public void finish() throws AlgebricksException {
                        if (count == 0) {
                            if (GlobalConfig.DEBUG) {
                                GlobalConfig.ASTERIX_LOGGER.finest("AVG aggregate ran over empty input.");
                            }
                        } else {
                            try {
                                if (metNull) {
                                    sumBytes.reset();
                                    nullSerde.serialize(ANull.NULL, sumBytesOutput);
                                } else {
                                    sumBytes.reset();
                                    aDouble.setValue(sum);
                                    doubleSerde.serialize(aDouble, sumBytesOutput);
                                }
                                countBytes.reset();
                                aInt32.setValue(count);
                                int32Serde.serialize(aInt32, countBytesOutput);
                                recordEval.evaluate(null);
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            }
                        }
                    }

                    @Override
                    public void finishPartial() throws AlgebricksException {
                        finish();
                    }
                };
            }
        };
    }

}
