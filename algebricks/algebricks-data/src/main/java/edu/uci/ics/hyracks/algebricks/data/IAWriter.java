package edu.uci.ics.hyracks.algebricks.data;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public interface IAWriter {

    public void init() throws AlgebricksException;

    public void printTuple(IFrameTupleAccessor tAccess, int tIdx) throws AlgebricksException;
}
