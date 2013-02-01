package edu.uci.ics.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.runningaggregates.base.AbstractRunningAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TidRunningAggregateDescriptor extends AbstractRunningAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TidRunningAggregateDescriptor();
        }
    };

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {

        return new ICopyRunningAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            public ICopyRunningAggregateFunction createRunningAggregateFunction(IDataOutputProvider provider)
                    throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new ICopyRunningAggregateFunction() {

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
        return AsterixBuiltinFunctions.TID;
    }

}
