package edu.uci.ics.hyracks.algebricks.runtime.aggregators;

import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TupleCountAggregateFunctionFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public IAggregateEvaluator createAggregateEvaluator() throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        return new IAggregateEvaluator() {

            int cnt;

            @Override
            public void step(IFrameTupleReference tuple) throws AlgebricksException {
                ++cnt;
            }

            @Override
            public void init() throws AlgebricksException {
                cnt = 0;
            }

            @Override
            public void finish(IPointable result) throws AlgebricksException {
                try {
                    abvs.reset();
                    abvs.getDataOutput().writeInt(cnt);
                    result.set(abvs);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
