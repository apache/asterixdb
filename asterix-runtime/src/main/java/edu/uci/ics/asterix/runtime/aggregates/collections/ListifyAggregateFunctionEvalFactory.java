package edu.uci.ics.asterix.runtime.aggregates.collections;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ListifyAggregateFunctionEvalFactory implements ICopyAggregateFunctionFactory {

    private static final long serialVersionUID = 1L;
    private ICopyEvaluatorFactory[] args;
    private final AOrderedListType orderedlistType;

    public ListifyAggregateFunctionEvalFactory(ICopyEvaluatorFactory[] args, AOrderedListType type) {
        this.args = args;
        this.orderedlistType = type;
    }

    @Override
    public ICopyAggregateFunction createAggregateFunction(final IDataOutputProvider provider) throws AlgebricksException {

        return new ICopyAggregateFunction() {

            private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
            private ICopyEvaluator eval = args[0].createEvaluator(inputVal);
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
