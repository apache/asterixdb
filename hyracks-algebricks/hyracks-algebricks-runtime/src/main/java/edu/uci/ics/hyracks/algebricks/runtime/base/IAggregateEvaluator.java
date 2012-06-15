package edu.uci.ics.hyracks.algebricks.runtime.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface IAggregateEvaluator {
    public void init() throws AlgebricksException;

    public void step(IFrameTupleReference tuple) throws AlgebricksException;

    public void finish(IPointable result) throws AlgebricksException;

    public void finishPartial() throws AlgebricksException;
}