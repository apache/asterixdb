package edu.uci.ics.asterix.runtime.aggregates.stream;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class NonEmptyStreamAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "non-empty-stream", 0, true);

    @Override
    public IAggregateFunctionFactory createAggregateFunctionFactory(IEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateFunction createAggregateFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {

                return new IAggregateFunction() {

                    private DataOutput out = provider.getDataOutput();
                    @SuppressWarnings("unchecked")
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
