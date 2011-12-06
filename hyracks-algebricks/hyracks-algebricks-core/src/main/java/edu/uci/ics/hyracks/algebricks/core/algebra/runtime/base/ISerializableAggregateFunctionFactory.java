package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public interface ISerializableAggregateFunctionFactory extends Serializable {
    public ISerializableAggregateFunction createAggregateFunction() throws AlgebricksException;
}
