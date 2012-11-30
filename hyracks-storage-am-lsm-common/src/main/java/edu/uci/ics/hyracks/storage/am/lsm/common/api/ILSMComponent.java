package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentState;

public interface ILSMComponent {
    public void activate();

    public void deactivate();

    public void threadEnter();

    public void threadExit();

    public void setState(LSMComponentState state);

    public LSMComponentState getState();
}
