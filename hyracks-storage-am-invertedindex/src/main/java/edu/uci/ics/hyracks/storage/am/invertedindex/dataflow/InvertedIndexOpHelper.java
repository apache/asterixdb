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
package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.IndexUtils;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public final class InvertedIndexOpHelper {

	private final TreeIndexOpHelper btreeOpHelper;
	
    private InvertedIndex invIndex;
    private int invIndexFileId = -1;
    private int partition;

    private IInvertedIndexOperatorDescriptorHelper opDesc;
    private IHyracksTaskContext ctx;

	public InvertedIndexOpHelper(TreeIndexOpHelper btreeOpHelper,
			IInvertedIndexOperatorDescriptorHelper opDesc,
			final IHyracksTaskContext ctx, int partition) {
		this.btreeOpHelper = btreeOpHelper;
		this.opDesc = opDesc;
		this.ctx = ctx;
		this.partition = partition;
	}

	// TODO: This is very similar to TreeIndexOpHelper. Maybe we can somehow merge them?
    public void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
        IFileSplitProvider fileSplitProvider = opDesc.getInvListsFileSplitProvider();

        FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        boolean fileIsMapped = fileMapProvider.isMapped(f);
        if (!fileIsMapped) {
            bufferCache.createFile(f);
        }
        int fileId = fileMapProvider.lookupFileId(f);
        try {
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            // Revert state of buffer cache since file failed to open.
            if (!fileIsMapped) {
                bufferCache.deleteFile(fileId);
            }
            throw e;
        }

		// only set btreeFileId member when openFile() succeeds,
		// otherwise deinit() will try to close the file that failed to open
		invIndexFileId = fileId;
		IndexRegistry<InvertedIndex> invIndexRegistry = opDesc
				.getInvIndexRegistryProvider().getRegistry(ctx);
		// create new inverted index and register it
		invIndexRegistry.lock();
		try {
			// check if inverted index has already been registered by
			// another thread
			invIndex = invIndexRegistry.get(invIndexFileId);
			if (invIndex == null) {
				// Create and register the inverted index.
				MultiComparator cmp = IndexUtils.createMultiComparator(opDesc
						.getInvListsComparatorFactories());
				// Assumes btreeOpHelper.init() has already been called.
				BTree btree = (BTree) btreeOpHelper.getTreeIndex();
				invIndex = new InvertedIndex(bufferCache, btree,
						opDesc.getInvListsTypeTraits(), cmp);
				invIndex.open(invIndexFileId);
				invIndexRegistry.register(invIndexFileId, invIndex);
			}
		} finally {
			invIndexRegistry.unlock();
		}
	}

    public void deinit() throws HyracksDataException {
        if (invIndexFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(invIndexFileId);
        }
    }

    public InvertedIndex getInvIndex() {
        return invIndex;
    }

    public ITreeIndexOperatorDescriptorHelper getOperatorDescriptor() {
        return opDesc;
    }

    public int getInvIndexFileId() {
        return invIndexFileId;
    }
}