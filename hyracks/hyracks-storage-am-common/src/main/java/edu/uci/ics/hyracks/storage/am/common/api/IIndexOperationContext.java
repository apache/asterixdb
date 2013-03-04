package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;

public interface IIndexOperationContext {
    void setOperation(IndexOperation newOp);
    
    IndexOperation getOperation();

    void reset();
}
