package edu.uci.ics.hyracks.algebricks.core.algebra.data;

import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public interface IAWriter {

    public void init() throws AlgebricksException;

    public void printTuple(FrameTupleAccessor tAccess, int tIdx) throws AlgebricksException;
}
