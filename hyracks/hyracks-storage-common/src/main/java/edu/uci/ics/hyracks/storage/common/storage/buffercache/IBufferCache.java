package edu.uci.ics.hyracks.storage.common.storage.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IBufferCache {
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    public void unpin(ICachedPage page) throws HyracksDataException;

    public int getPageSize();

    public int getNumPages();

    public void close();
}