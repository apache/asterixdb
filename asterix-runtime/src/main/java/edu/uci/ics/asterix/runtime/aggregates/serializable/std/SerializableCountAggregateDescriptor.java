package edu.uci.ics.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractSerializableAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * NULLs are also counted.
 */
public class SerializableCountAggregateDescriptor extends AbstractSerializableAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count-serial",
            1, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SerializableCountAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopySerializableAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopySerializableAggregateFunction createAggregateFunction() throws AlgebricksException {

                return new ICopySerializableAggregateFunction() {
                    private AMutableInt32 result = new AMutableInt32(-1);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);

                    @Override
                    public void init(DataOutput state) throws AlgebricksException {
                        try {
                            state.writeInt(0);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public void step(IFrameTupleReference tuple, byte[] state, int start, int len)
                            throws AlgebricksException {
                        int cnt = BufferSerDeUtil.getInt(state, start);
                        cnt++;
                        BufferSerDeUtil.writeInt(cnt, state, start);
                    }

                    @Override
                    public void finish(byte[] state, int start, int len, DataOutput out) throws AlgebricksException {
                        int cnt = BufferSerDeUtil.getInt(state, start);
                        try {
                            result.setValue(cnt);
                            int32Serde.serialize(result, out);
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
