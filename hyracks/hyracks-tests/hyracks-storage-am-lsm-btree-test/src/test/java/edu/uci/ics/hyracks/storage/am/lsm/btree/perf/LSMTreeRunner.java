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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMTreeRunner implements IExperimentRunner {

    private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
    private static final int HYRACKS_FRAME_SIZE = 131072;

    protected IHyracksTaskContext ctx;
    protected IOManager ioManager;
    protected int ioDeviceId;
    protected IBufferCache bufferCache;
    protected int lsmtreeFileId;

    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected final static String classDir = "/tmp/lsmtree/";
    protected String onDiskDir;
    protected FileReference file;

    protected final int numBatches;
    protected final LSMBTree lsmtree;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected IBufferCache memBufferCache;
    private final int onDiskPageSize;
    private final int onDiskNumPages;
    private final static ThreadFactory threadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    public LSMTreeRunner(int numBatches, int inMemPageSize, int inMemNumPages, int onDiskPageSize, int onDiskNumPages,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields,
            double bloomFilterFalsePositiveRate) throws BTreeException, HyracksException {
        this.numBatches = numBatches;

        this.onDiskPageSize = onDiskPageSize;
        this.onDiskNumPages = onDiskNumPages;

        onDiskDir = classDir + sep + simpleDateFormat.format(new Date()) + sep;
        file = new FileReference(new File(onDiskDir));
        ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

        TestStorageManagerComponentHolder.init(this.onDiskPageSize, this.onDiskNumPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        ioManager = TestStorageManagerComponentHolder.getIOManager();
        ioDeviceId = 0;
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);

        List<IVirtualBufferCache> virtualBufferCaches = new ArrayList<IVirtualBufferCache>();
        for (int i = 0; i < 2; i++) {
            IVirtualBufferCache virtualBufferCache = new VirtualBufferCache(new HeapBufferAllocator(), inMemPageSize,
                    inMemNumPages / 2);
            virtualBufferCaches.add(virtualBufferCache);
        }

        this.ioScheduler = AsynchronousScheduler.INSTANCE;
        AsynchronousScheduler.INSTANCE.init(threadFactory);

        lsmtree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, bufferCache, fmp, typeTraits, cmpFactories,
                bloomFilterKeyFields, bloomFilterFalsePositiveRate, NoMergePolicy.INSTANCE,
                new ThreadCountingTracker(), ioScheduler, NoOpIOOperationCallback.INSTANCE);
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
        LSMTreeThread[] threads = new LSMTreeThread[numThreads];
        int threadNumBatches = numBatches / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new LSMTreeThread(dataGen, lsmtree, threadNumBatches);
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
    public void reset() throws Exception {
        try {
            lsmtree.deactivate();
        } catch (HyracksDataException e) {
            // ignore
        }
        try {
            lsmtree.destroy();
        } catch (HyracksDataException e) {
            // ignore
        }

        lsmtree.create();
        lsmtree.activate();
    }

    @Override
    public void deinit() throws Exception {
    }

    public class LSMTreeThread extends Thread {
        private final DataGenThread dataGen;
        private final int numBatches;
        private final IIndexAccessor lsmTreeAccessor;

        public LSMTreeThread(DataGenThread dataGen, LSMBTree lsmTree, int numBatches) {
            this.dataGen = dataGen;
            this.numBatches = numBatches;
            lsmTreeAccessor = lsmTree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                            lsmTreeAccessor.insert(batch.get(j));
                        } catch (TreeIndexException e) {
                        }
                    }
                    dataGen.releaseBatch(batch);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
