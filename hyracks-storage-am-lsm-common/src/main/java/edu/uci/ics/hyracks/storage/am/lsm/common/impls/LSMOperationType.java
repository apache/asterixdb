package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

public enum LSMOperationType {
    SEARCH,
    MODIFICATION,
    FLUSH,
    MERGE,
    NOOP
}
