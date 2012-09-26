package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IOperationCallback {
    public void pre(ITupleReference tuple);

    public void post(ITupleReference tuple);
}
