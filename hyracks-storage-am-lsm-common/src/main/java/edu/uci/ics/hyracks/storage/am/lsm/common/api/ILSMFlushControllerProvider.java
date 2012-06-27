package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMFlushControllerProvider extends Serializable {
    public ILSMFlushController getFlushController();
}
