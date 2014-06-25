package edu.uci.ics.hyracks.control.common.shutdown;

public interface IShutdownStatusConditionVariable {
    /**
     * @return true if all nodes ack shutdown
     * @throws Exception
     */
    public boolean waitForCompletion() throws Exception;

}
