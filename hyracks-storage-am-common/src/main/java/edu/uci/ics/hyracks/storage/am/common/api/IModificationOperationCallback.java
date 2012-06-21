package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IModificationOperationCallback {
    public void before(ITupleReference tuple);

    public void commence(ITupleReference tuple);

    public void found(ITupleReference tuple);
}
