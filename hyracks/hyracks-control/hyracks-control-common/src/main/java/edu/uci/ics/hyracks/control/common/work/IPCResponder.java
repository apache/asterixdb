package edu.uci.ics.hyracks.control.common.work;

import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.exceptions.IPCException;

public class IPCResponder<T> implements IResultCallback<T> {
    private final IIPCHandle handle;

    private final long rmid;

    public IPCResponder(IIPCHandle handle, long rmid) {
        this.handle = handle;
        this.rmid = rmid;
    }

    @Override
    public void setValue(T result) {
        try {
            handle.send(rmid, result, null);
        } catch (IPCException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setException(Exception e) {
        try {
            handle.send(rmid, null, e);
        } catch (IPCException e1) {
            e1.printStackTrace();
        }
    }
}