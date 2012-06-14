package edu.uci.ics.asterix.runtime.aggregates.stream;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class NonEmptyStreamAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "non-empty-stream", 0, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NonEmptyStreamAggregateDescriptor();
        }
    };

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyAggregateFunction createAggregateFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {

                return new ICopyAggregateFunction() {

                    private DataOutput out = provider.getDataOutput();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    boolean res = false;

                    @Override
                    public void init() throws AlgebricksException {
                        res = false;
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        res = true;
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public void finish() throws AlgebricksException {
                        ABoolean b = res ? ABoolean.TRUE : ABoolean.FALSE;
                        try {
                            serde.serialize(b, out);
                        } catch (HyracksDataException e) {
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

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
