package edu.uci.ics.hyracks.storage.am.lsm.common.api;

public interface ILSMIOScheduler {
    public void scheduleFlush(ILSMIndex index);

    public void scheduleMerge(ILSMIndex index);
}
