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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexIdProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.impls.IndexIdProvider;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class IndexDataflowHelper {
    protected IIndex index;
    protected int indexFileId = -1;
    protected int partition;

    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IOperationCallbackProvider opCallbackProvider;
    protected final IIndexIdProvider indexIdProvider;
    protected final boolean createIfNotExists;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx,
            IOperationCallbackProvider opCallbackProvider, IIndexIdProvider indexIdProvider, int partition,
            boolean createIfNotExists) {
        this.opCallbackProvider = opCallbackProvider;
        this.indexIdProvider = indexIdProvider;
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.partition = partition;
        this.createIfNotExists = createIfNotExists;
    }

    public void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);

        FileReference f = getFilereference();
        int fileId = -1;
        synchronized (fileMapProvider) {
            boolean fileIsMapped = fileMapProvider.isMapped(f);
            if (!fileIsMapped) {
                bufferCache.createFile(f);
            }
            fileId = fileMapProvider.lookupFileId(f);
            try {
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        // get indexId
        // TODO this is temporary code that should be changed if the resourceId is changed to includes indexId and partitionId
        int indexId = byteArrayToInt(indexIdProvider.getIndexId(), 0);

        // Only set indexFileId member when openFile() succeeds,
        // otherwise deinit() will try to close the file that failed to open
        indexFileId = fileId;
        IndexRegistry<IIndex> indexRegistry = opDesc.getIndexRegistryProvider().getRegistry(ctx);
        // Create new index instance and register it.
        synchronized (indexRegistry) {
            // Check if the index has already been registered.
            index = indexRegistry.get(indexId);
            if (index != null) {
                return;
            }
            index = createIndexInstance();
            if (createIfNotExists) {
                index.create(indexFileId);
            }
            index.open(indexFileId);
            indexRegistry.register(indexId, index);
        }
    }

    public abstract IIndex createIndexInstance() throws HyracksDataException;

    public FileReference getFilereference() {
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        /* TODO 
         * This commented-out code is the correct code to enable multiple index instances in one NC.
         * This code will become alive when the resourceId(which is indexId) consists of partitionId and indexId.
         * 
         * return fileSplitProvider.getFileSplits()[partition].getLocalFile();
         */
        // temporary code for the course project
        return fileSplitProvider.getFileSplits()[0].getLocalFile();

    }

    public void deinit() throws HyracksDataException {
        if (indexFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(indexFileId);
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
        return opCallbackProvider;
    }

    private int byteArrayToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
}
