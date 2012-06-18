package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMMergePolicyProvider extends Serializable {
    public ILSMMergePolicy getMergePolicy();
}
