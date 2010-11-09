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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

final class BTreeOpHelper {
    private IBTreeInteriorFrame interiorFrame;
    private IBTreeLeafFrame leafFrame;

    private BTree btree;
    private int btreeFileId = -1;
    private int partition;
    
    private AbstractBTreeOperatorDescriptor opDesc;
    private IHyracksContext ctx;

    private boolean createBTree;
    
    BTreeOpHelper(AbstractBTreeOperatorDescriptor opDesc, final IHyracksContext ctx, int partition, boolean createBTree) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.createBTree = createBTree;
        this.partition = partition;
    }  
    
    void init() throws HyracksDataException {
    	
    	IBufferCache bufferCache = opDesc.getBufferCacheProvider().getBufferCache();
        FileManager fileManager = opDesc.getBufferCacheProvider().getFileManager();
        IFileMappingProviderProvider fileMappingProviderProvider = opDesc.getFileMappingProviderProvider();               
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        
        File f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        if(!f.exists()) {
        	File dir = new File(f.getParent());        	
        	dir.mkdirs();
        }
        RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");
		} catch (FileNotFoundException e) {
			throw new HyracksDataException(e.getMessage());
		}
        
        String fileName = f.getAbsolutePath();
        Integer fileId = fileMappingProviderProvider.getFileMappingProvider().getFileId(fileName);        
        if(fileId == null) {
        	if(createBTree) {
        		fileId = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId(fileName, createBTree);        		
        	}
        	else {
        		throw new HyracksDataException("Cannot get id for file " + fileName + ". File name has not been mapped.");
        	}
        }
        else {
        	if(createBTree) {
        		throw new HyracksDataException("Cannot map file " + fileName + " to an id. File name has already been mapped.");
        	}        	     
        }        
        btreeFileId = fileId;  
        
        if (!f.exists() && !createBTree) {
            throw new HyracksDataException("Trying to open btree from file " + fileName + " but file doesn't exist.");
        }
        
        if(createBTree) {
        	FileInfo fi = new FileInfo(btreeFileId, raf);
        	fileManager.registerFile(fi);
        }
        
        interiorFrame = opDesc.getInteriorFactory().getFrame();
        leafFrame = opDesc.getLeafFactory().getFrame();

        BTreeRegistry btreeRegistry = opDesc.getBtreeRegistryProvider().getBTreeRegistry();
        btree = btreeRegistry.get(btreeFileId);
        if (btree == null) {
        	
            // create new btree and register it            
            btreeRegistry.lock();
            try {
                // check if btree has already been registered by another thread
                btree = btreeRegistry.get(btreeFileId);
                if (btree == null) {
                    // this thread should create and register the btree
                	                   
                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getComparatorFactories().length];
                    for (int i = 0; i < opDesc.getComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getComparatorFactories()[i].createBinaryComparator();
                    }

                    MultiComparator cmp = new MultiComparator(opDesc.getFieldCount(), comparators);
                    
                    btree = new BTree(bufferCache, opDesc.getInteriorFactory(), opDesc.getLeafFactory(), cmp);
                    if (createBTree) {
                        MetaDataFrame metaFrame = new MetaDataFrame();
                        try {
							btree.create(btreeFileId, leafFrame, metaFrame);
						} catch (Exception e) {
							throw new HyracksDataException(e.getMessage());
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
    
    public BTree getBTree() {
        return btree;
    }

    public IHyracksContext getHyracksContext() {
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