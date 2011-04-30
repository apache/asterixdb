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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

final class BTreeOpHelper {

	public enum BTreeMode {
		OPEN_BTREE, CREATE_BTREE, ENLIST_BTREE
	}
	
	private IBTreeInteriorFrame interiorFrame;
	private IBTreeLeafFrame leafFrame;

	private BTree btree;
	private int btreeFileId = -1;
	private int partition;

    private AbstractBTreeOperatorDescriptor opDesc;
    private IHyracksStageletContext ctx;

	private BTreeMode mode;

    BTreeOpHelper(AbstractBTreeOperatorDescriptor opDesc, final IHyracksStageletContext ctx, int partition,
            BTreeMode mode) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.mode = mode;
        this.partition = partition;
    }

	void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();

        FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        boolean fileIsMapped = fileMapProvider.isMapped(f);

		switch (mode) {
		
		case OPEN_BTREE: {
			if (!fileIsMapped) {
				throw new HyracksDataException(
						"Trying to open btree from unmapped file " + f.toString());
			}
		}
		break;

		case CREATE_BTREE:
		case ENLIST_BTREE: {
			if (!fileIsMapped) {
				bufferCache.createFile(f);
			}
		}
		break;
		
		}
		
        int fileId = fileMapProvider.lookupFileId(f);		
        try {
        	bufferCache.openFile(fileId);
        } catch(HyracksDataException e) {
        	// revert state of buffer cache since file failed to open
        	if(!fileIsMapped) {
        		bufferCache.deleteFile(fileId);
        	}
        	throw e;
        }
        
        // only set btreeFileId member when openFile() succeeds, 
        // otherwise deinit() will try to close the file that failed to open
        btreeFileId = fileId;

		interiorFrame = opDesc.getInteriorFactory().getFrame();
		leafFrame = opDesc.getLeafFactory().getFrame();

        BTreeRegistry btreeRegistry = opDesc.getBtreeRegistryProvider().getBTreeRegistry(ctx);
        btree = btreeRegistry.get(btreeFileId);
        if (btree == null) {

			// create new btree and register it
			btreeRegistry.lock();
			try {
				// check if btree has already been registered by another thread
				btree = btreeRegistry.get(btreeFileId);
				if (btree == null) {
					// this thread should create and register the btree

					IBinaryComparator[] comparators = new IBinaryComparator[opDesc
							.getComparatorFactories().length];
					for (int i = 0; i < opDesc.getComparatorFactories().length; i++) {
						comparators[i] = opDesc.getComparatorFactories()[i]
								.createBinaryComparator();
					}

					MultiComparator cmp = new MultiComparator(opDesc
							.getTypeTraits(), comparators);

					// TODO: abstract away in some kind of factory
					ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
					IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, btreeFileId, 0, metaDataFrameFactory);
					btree = new BTree(bufferCache, freePageManager, opDesc.getInteriorFactory(),
							opDesc.getLeafFactory(), cmp);
					if (mode == BTreeMode.CREATE_BTREE) {
						ITreeIndexMetaDataFrame metaFrame = btree.getFreePageManager().getMetaDataFrameFactory().getFrame();
						try {
							btree.create(btreeFileId, leafFrame, metaFrame);							
						} catch (Exception e) {
							throw new HyracksDataException(e);
						}
					}
					btree.open(btreeFileId);
					btreeRegistry.register(btreeFileId, btree);
				}
			} finally {
				btreeRegistry.unlock();
			}
		}
	}

    public void deinit() throws HyracksDataException {
        if (btreeFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(btreeFileId);
        }
    }

	public BTree getBTree() {
		return btree;
	}

    public IHyracksStageletContext getHyracksStageletContext() {
        return ctx;
    }

	public AbstractBTreeOperatorDescriptor getOperatorDescriptor() {
		return opDesc;
	}

	public IBTreeLeafFrame getLeafFrame() {
		return leafFrame;
	}

	public IBTreeInteriorFrame getInteriorFrame() {
		return interiorFrame;
	}

	public int getBTreeFileId() {
		return btreeFileId;
	}
}