package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IModificationOperationCallbackFactory extends Serializable {
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, IHyracksTaskContext ctx)
            throws HyracksDataException;
}
