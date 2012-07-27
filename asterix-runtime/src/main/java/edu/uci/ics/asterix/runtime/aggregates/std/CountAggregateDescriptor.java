package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * NULLs are also counted.
 */
public class CountAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CountAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyAggregateFunction createAggregateFunction(IDataOutputProvider provider) throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new ICopyAggregateFunction() {
                    private AMutableInt32 result = new AMutableInt32(-1);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    private int cnt;

                    @Override
                    public void init() {
                        cnt = 0;
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        cnt++;
                    }

                    @Override
                    public void finish() throws AlgebricksException {
                        try {
                            result.setValue(cnt);
                            int32Serde.serialize(result, out);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
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
