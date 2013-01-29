package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

public enum LSMOperationType {
    SEARCH,
    MODIFICATION,
    FORCE_MODIFICATION,
    FLUSH,
    MERGE,
    NOOP
}
