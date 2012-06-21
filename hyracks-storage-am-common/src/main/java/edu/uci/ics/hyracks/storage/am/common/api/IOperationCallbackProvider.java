package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

public interface IOperationCallbackProvider extends Serializable {
    public IModificationOperationCallback getModificationOperationCallback();

    public ISearchOperationCallback getSearchOperationCallback();
}
