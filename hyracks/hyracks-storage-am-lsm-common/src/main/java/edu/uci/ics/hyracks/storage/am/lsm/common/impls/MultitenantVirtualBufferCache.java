package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class MultitenantVirtualBufferCache implements IVirtualBufferCache {

    private final IVirtualBufferCache vbc;
    private int openCount;

    public MultitenantVirtualBufferCache(IVirtualBufferCache virtualBufferCache) {
        this.vbc = virtualBufferCache;
        openCount = 0;
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        vbc.createFile(fileRef);
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        vbc.openFile(fileId);
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        vbc.closeFile(fileId);
    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        vbc.deleteFile(fileId, flushDirtyPages);
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return vbc.tryPin(dpid);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        return vbc.pin(dpid, newPage);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        vbc.unpin(page);
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        vbc.flushDirtyPage(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        vbc.force(fileId, metadata);
    }

    @Override
    public int getPageSize() {
        return vbc.getPageSize();
    }

    @Override
    public int getNumPages() {
        return vbc.getNumPages();
    }

    @Override
    public synchronized void close() {
        --openCount;
        if (openCount == 0) {
            vbc.close();
        }
    }

    @Override
    public synchronized void open() {
        openCount++;
        vbc.open();
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public void reset() {
        vbc.reset();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return vbc.getFileMapProvider();
    }

}
