package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentState;

public interface ILSMComponent {
    public void activate();

    public void deactivate();

    public void threadEnter();

    public void threadExit();

    public int getThreadReferenceCount();

    public void setState(LSMComponentState state);

    public boolean negativeCompareAndSet(LSMComponentState compare, LSMComponentState update);

    public LSMComponentState getState();

    // TODO: create two interfaces one for immutable and another for mutable components.

    // Only for immutable components.
    public void destroy() throws HyracksDataException;

    // Only for mutable components.
    public void reset() throws HyracksDataException;
}
