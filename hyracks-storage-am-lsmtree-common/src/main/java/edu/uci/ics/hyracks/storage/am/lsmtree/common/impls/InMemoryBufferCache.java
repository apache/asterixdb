package edu.uci.ics.hyracks.storage.am.lsmtree.common.impls;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCacheInternal;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPageInternal;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class InMemoryBufferCache implements IBufferCacheInternal {
	
	private final int pageSize;
	private final int numPages;
    private final CachedPage[] cachedPages;

	//Constructor
	public InMemoryBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages){
		
        this.pageSize = pageSize;
        this.numPages = numPages;
		ByteBuffer[] buffers = allocator.allocate(this.pageSize, this.numPages);
		cachedPages = new CachedPage[buffers.length];
        for (int i = 0; i < buffers.length; ++i) {
            cachedPages[i] = new CachedPage(i, buffers[i]);
        }
	}
	
	@Override
	public void createFile(FileReference fileRef) throws HyracksDataException {
		// Do nothing
	}

	@Override
	public void openFile(int fileId) throws HyracksDataException {
		// Do nothing		
	}

	@Override
	public void closeFile(int fileId) throws HyracksDataException {
		// Do nothing
	}

	@Override
	public void deleteFile(int fileId) throws HyracksDataException {
		// Do nothing
	}

	@Override
	public ICachedPage tryPin(long dpid) throws HyracksDataException {
		// Just call pin!
		return null;
	}

	@Override
	public ICachedPage pin(long dpid, boolean newPage){
		return cachedPages[BufferedFileHandle.getPageId(dpid)];
	}

	@Override
	public void unpin(ICachedPage page) throws HyracksDataException {
		//Do Nothing
	}

	@Override
	public int getPageSize() {
		return pageSize;
	}

	@Override
	public int getNumPages() {
		return numPages;
	}

	@Override
	public void close() {
		// Do nothing	
	}

	@Override
	public ICachedPageInternal getPage(int cpid) {
		return cachedPages[cpid];
	}

    private class CachedPage implements ICachedPageInternal {
        private final int cpid;
        private final ByteBuffer buffer;
        private final ReadWriteLock latch;

        public CachedPage(int cpid, ByteBuffer buffer) {
            this.cpid = cpid;
            this.buffer = buffer;
            latch = new ReentrantReadWriteLock(true);
        }

        @Override
        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        public Object getReplacementStrategyObject() {
        	//Do nothing
            return null;
        }

        @Override
        public boolean pinIfGoodVictim() {
        	//Do nothing
        	return false;
        }

        @Override
        public int getCachedPageId() {
            return cpid;
        }

        @Override
        public void acquireReadLatch() {
            latch.readLock().lock();
        }

        private void acquireWriteLatch(boolean markDirty) {
            latch.writeLock().lock();
        }

        @Override
        public void acquireWriteLatch() {
            acquireWriteLatch(true);
        }

        @Override
        public void releaseReadLatch() {
            latch.readLock().unlock();
        }

        @Override
        public void releaseWriteLatch() {
            latch.writeLock().unlock();
        }
    }
}
