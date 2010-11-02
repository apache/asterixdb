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
import java.io.RandomAccessFile;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
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
    
    void init() throws Exception {
    	
    	IBufferCache bufferCache = opDesc.getBufferCacheProvider().getBufferCache();
        FileManager fileManager = opDesc.getBufferCacheProvider().getFileManager();
        IFileMappingProviderProvider fileMappingProviderProvider = opDesc.getFileMappingProviderProvider();               
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        
        //String ncDataPath = System.getProperty("NodeControllerDataPath");
        File f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        if(!f.exists()) {
        	File dir = new File(f.getParent());        	
        	dir.mkdirs();
        }
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        
        String fileName = f.getAbsolutePath();
        btreeFileId = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId(fileName, createBTree);     
        
        if (!f.exists() && !createBTree) {
            throw new Exception("Trying to open btree from file " + fileName + " but file doesn't exist.");
        }
        
        try {
            FileInfo fi = new FileInfo(btreeFileId, raf);
            fileManager.registerFile(fi);
        } catch (Exception e) {
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
                    // this thread should create and register the btee
                	                   
                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getComparatorFactories().length];
                    for (int i = 0; i < opDesc.getComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getComparatorFactories()[i].createBinaryComparator();
                    }

                    MultiComparator cmp = new MultiComparator(opDesc.getFieldCount(), comparators);
                    
                    btree = new BTree(bufferCache, opDesc.getInteriorFactory(), opDesc.getLeafFactory(), cmp);
                    if (createBTree) {
                        MetaDataFrame metaFrame = new MetaDataFrame();
                        btree.create(btreeFileId, leafFrame, metaFrame);
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