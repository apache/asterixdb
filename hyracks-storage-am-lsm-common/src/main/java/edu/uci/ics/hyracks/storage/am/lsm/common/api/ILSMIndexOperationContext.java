package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;

public interface ILSMIndexOperationContext extends IIndexOperationContext {
    public ISearchOperationCallback getSearchOperationCallback();
    public IModificationOperationCallback getModificationCallback();
}
