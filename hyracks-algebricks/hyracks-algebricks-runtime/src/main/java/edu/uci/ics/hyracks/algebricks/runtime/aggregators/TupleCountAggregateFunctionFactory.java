package edu.uci.ics.hyracks.algebricks.runtime.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TupleCountAggregateFunctionFactory implements ICopyAggregateFunctionFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public ICopyAggregateFunction createAggregateFunction(IDataOutputProvider provider) throws AlgebricksException {

        final DataOutput out = provider.getDataOutput();
        return new ICopyAggregateFunction() {

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
            public void finish() throws AlgebricksException {
                try {
                    out.writeInt(cnt);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }

            @Override
            public void finishPartial() throws AlgebricksException {
                try {
                    out.writeInt(cnt);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
