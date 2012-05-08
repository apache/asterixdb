package edu.uci.ics.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.runningaggregates.base.AbstractRunningAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TidRunningAggregateDescriptor extends AbstractRunningAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tid", 0, true);

    @Override
    public IRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(IEvaluatorFactory[] args)
            throws AlgebricksException {

        return new IRunningAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            public IRunningAggregateFunction createRunningAggregateFunction(IDataOutputProvider provider)
                    throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new IRunningAggregateFunction() {

                    int cnt;
                    ISerializerDeserializer<AInt32> serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    AMutableInt32 m = new AMutableInt32(0);

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            m.setValue(cnt);
                            serde.serialize(m, out);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }

                        ++cnt;
                    }

                    @Override
                    public void init() throws AlgebricksException {
                        cnt = 0;
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
