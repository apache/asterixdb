package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * NULLs are also counted.
 */
public class CountAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1,
            true);

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
            public IAggregateFunction createAggregateFunction(IDataOutputProvider provider) throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new IAggregateFunction() {
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
