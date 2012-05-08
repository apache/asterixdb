package edu.uci.ics.asterix.runtime.aggregates.collections;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ListifyAggregateFunctionEvalFactory implements IAggregateFunctionFactory {

    private static final long serialVersionUID = 1L;
    private IEvaluatorFactory[] args;
    private final AOrderedListType orderedlistType;

    public ListifyAggregateFunctionEvalFactory(IEvaluatorFactory[] args, AOrderedListType type) {
        this.args = args;
        this.orderedlistType = type;
    }

    @Override
    public IAggregateFunction createAggregateFunction(final IDataOutputProvider provider) throws AlgebricksException {

        return new IAggregateFunction() {

            private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
            private IEvaluator eval = args[0].createEvaluator(inputVal);
            private DataOutput out = provider.getDataOutput();
            private OrderedListBuilder builder = new OrderedListBuilder();

            @Override
            public void init() throws AlgebricksException {
                try {
                    builder.reset(orderedlistType);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            @Override
            public void step(IFrameTupleReference tuple) throws AlgebricksException {
                try {
                    inputVal.reset();
                    eval.evaluate(tuple);
                    builder.addItem(inputVal);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }

            @Override
            public void finish() throws AlgebricksException {
                try {
                    builder.write(out, true);
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

}
