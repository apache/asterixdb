/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BufferCacheProvider implements IBufferCacheProvider {
		
	private static final long serialVersionUID = 1L;
	
	private static IBufferCache bufferCache = null;	
	private static FileManager fileManager = null;
	private static final int PAGE_SIZE = 8192;
    private static final int NUM_PAGES = 40;
	
	@Override
	public synchronized IBufferCache getBufferCache() {
		
		if(bufferCache == null) {
			if(fileManager == null) fileManager = new FileManager();			
	        ICacheMemoryAllocator allocator = new BufferAllocator();
	        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
	        bufferCache = new BufferCache(allocator, prs, fileManager, PAGE_SIZE, NUM_PAGES);
		}
		
		return bufferCache;
	}
	
	@Override
	public synchronized FileManager getFileManager() {
		if(fileManager == null) fileManager = new FileManager();
		return fileManager;
	}	
	
	public class BufferAllocator implements ICacheMemoryAllocator {
        @Override
        public ByteBuffer[] allocate(int pageSize, int numPages) {
            ByteBuffer[] buffers = new ByteBuffer[numPages];
            for (int i = 0; i < numPages; ++i) {
                buffers[i] = ByteBuffer.allocate(pageSize);
            }
            return buffers;
        }
    }	
}
