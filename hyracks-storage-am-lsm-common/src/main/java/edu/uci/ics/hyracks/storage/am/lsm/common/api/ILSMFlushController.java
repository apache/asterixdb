package edu.uci.ics.hyracks.storage.am.lsm.common.api;

public interface ILSMFlushController {
    public void setFlushStatus(ILSMIndex index, boolean needsFlush);

    public boolean getFlushStatus(ILSMIndex index);
}