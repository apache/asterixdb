package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;

public interface IIndexOperationContext {
    void startOperation(IndexOperation newOp);

    void reset();
}
