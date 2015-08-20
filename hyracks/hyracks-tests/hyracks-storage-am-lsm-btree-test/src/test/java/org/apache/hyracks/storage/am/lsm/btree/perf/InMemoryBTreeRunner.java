/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

public class InMemoryBTreeRunner extends Thread implements IExperimentRunner {
    protected IBufferCache bufferCache;
    protected FileReference file;

    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String fileName;

    protected final int numBatches;
    protected BTree btree;

    public InMemoryBTreeRunner(int numBatches, int pageSize, int numPages, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories) throws HyracksDataException, BTreeException {
        this.numBatches = numBatches;
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        file = new FileReference(new File(fileName));
        init(pageSize, numPages, typeTraits, cmpFactories);
    }

    protected void init(int pageSize, int numPages, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories)
            throws HyracksDataException, BTreeException {
        bufferCache = new VirtualBufferCache(new HeapBufferAllocator(), pageSize, numPages);
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        IFreePageManager freePageManager = new VirtualFreePageManager(bufferCache.getNumPages());
        btree = new BTree(bufferCache, new TransientFileMapManager(), freePageManager, interiorFrameFactory,
                leafFrameFactory, cmpFactories, typeTraits.length, file);
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
        BTreeThread[] threads = new BTreeThread[numThreads];
        int threadNumBatches = numBatches / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BTreeThread(dataGen, btree, threadNumBatches);
        }
        // Wait until the tupleBatchQueue is completely full.
        while (dataGen.tupleBatchQueue.remainingCapacity() != 0) {
            Thread.sleep(10);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void deinit() throws Exception {
        bufferCache.close();
    }

    @Override
    public void reset() throws Exception {
        btree.create();
    }

    public class BTreeThread extends Thread {
        private final DataGenThread dataGen;
        private final int numBatches;
        private final ITreeIndexAccessor indexAccessor;

        public BTreeThread(DataGenThread dataGen, BTree btree, int numBatches) {
            this.dataGen = dataGen;
            this.numBatches = numBatches;
            indexAccessor = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                            indexAccessor.insert(batch.get(j));
                        } catch (TreeIndexException e) {
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
