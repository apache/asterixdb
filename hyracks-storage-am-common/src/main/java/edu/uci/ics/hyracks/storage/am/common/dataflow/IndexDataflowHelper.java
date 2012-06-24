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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class IndexDataflowHelper {
    protected IIndex index;
    protected int indexFileId = -1;
    
    protected final int partition;
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.partition = partition;
    }

    public void init(boolean forceCreate) throws HyracksDataException {
    	IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
    	IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
    	IndexRegistry<IIndex> indexRegistry = opDesc.getIndexRegistryProvider().getRegistry(ctx);
    	FileReference fileRef = getFilereference();
    	int fileId = -1;
    	boolean fileIsMapped = false;
    	synchronized (fileMapProvider) {
    		fileIsMapped = fileMapProvider.isMapped(fileRef);
    		if (!fileIsMapped) {
    			bufferCache.createFile(fileRef);
    		}            
    		fileId = fileMapProvider.lookupFileId(fileRef);
    		try {
    	    	// Also creates the file if it doesn't exist yet.
    			bufferCache.openFile(fileId);
    		} catch (HyracksDataException e) {
    			// Revert state of buffer cache since file failed to open.
    			if (!fileIsMapped) {
    				bufferCache.deleteFile(fileId, false);
    			}
    			throw e;
    		}
    	}
    	// Only set indexFileId member after openFile() succeeds.
    	indexFileId = fileId;    	
    	// Create new index instance and register it.
    	synchronized (indexRegistry) {
    		// Check if the index has already been registered.
    		boolean register = false;
    	    index = indexRegistry.get(indexFileId);
    		if (index == null) {
    		    index = createIndexInstance();
    		    register = true;
    		}
    		if (forceCreate) {
    		    index.create(indexFileId);
    		}
    		index.open(indexFileId);
    		if (register) {
    		    indexRegistry.register(indexFileId, index);
    		}
    	}
    }
    
    public abstract IIndex createIndexInstance() throws HyracksDataException;

    public FileReference getFilereference() {
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        return fileSplitProvider.getFileSplits()[partition].getLocalFile();
    }

    public void deinit() throws HyracksDataException {
        if (indexFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(indexFileId);
            indexFileId = -1;
        }
    }
    
    public IIndex getIndex() {
        return index;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }

    public IIndexOperatorDescriptor getOperatorDescriptor() {
        return opDesc;
    }

    public int getIndexFileId() {
        return indexFileId;
    }

    public IOperationCallbackProvider getOpCallbackProvider() {
        return opDesc.getOpCallbackProvider();
    }
}
