package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMFlushPolicyProvider extends Serializable {
    public ILSMFlushPolicy getFlushPolicy();
}
