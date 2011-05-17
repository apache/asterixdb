package edu.uci.ics.hyracks.control.common.job;

public enum PartitionState {
    STARTED,
    COMMITTED;

    public boolean isAtLeast(PartitionState minState) {
        switch (this) {
            case COMMITTED:
                return true;

            case STARTED:
                return minState == STARTED;
        }
        return false;
    }
}