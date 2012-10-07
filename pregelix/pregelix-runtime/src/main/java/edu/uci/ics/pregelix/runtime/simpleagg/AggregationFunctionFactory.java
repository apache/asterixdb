package edu.uci.ics.pregelix.runtime.simpleagg;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.runtime.base.IAggregateFunction;
import edu.uci.ics.pregelix.runtime.base.IAggregateFunctionFactory;

public class AggregationFunctionFactory implements IAggregateFunctionFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;
    private final boolean isFinalStage;

    public AggregationFunctionFactory(IConfigurationFactory confFactory, boolean isFinalStage) {
        this.confFactory = confFactory;
        this.isFinalStage = isFinalStage;
    }

    @Override
    public IAggregateFunction createAggregateFunction(IDataOutputProvider provider) throws HyracksException {
        DataOutput output = provider.getDataOutput();
        return new AggregationFunction(confFactory, output, isFinalStage);
    }
}
