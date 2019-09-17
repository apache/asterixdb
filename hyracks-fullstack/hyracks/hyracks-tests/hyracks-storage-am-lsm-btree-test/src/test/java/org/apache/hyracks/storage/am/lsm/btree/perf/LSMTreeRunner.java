/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.btree.perf;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.datagen.TupleBatch;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMTreeRunner implements IExperimentRunner {

    private static final Logger LOGGER = LogManager.getLogger();
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
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    public LSMTreeRunner(int numBatches, int inMemPageSize, int inMemNumPages, int onDiskPageSize, int onDiskNumPages,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields,
            double bloomFilterFalsePositiveRate) throws HyracksDataException {
        this.numBatches = numBatches;

        this.onDiskPageSize = onDiskPageSize;
        this.onDiskNumPages = onDiskNumPages;
        onDiskDir = classDir + sep + simpleDateFormat.format(new Date()) + sep;
        ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
        TestStorageManagerComponentHolder.init(this.onDiskPageSize, this.onDiskNumPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        ioManager = TestStorageManagerComponentHolder.getIOManager();

        ioDeviceId = 0;
        file = ioManager.resolveAbsolutePath(onDiskDir);

        List<IVirtualBufferCache> virtualBufferCaches = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            IVirtualBufferCache virtualBufferCache =
                    new VirtualBufferCache(new HeapBufferAllocator(), inMemPageSize, inMemNumPages / 2);
            virtualBufferCaches.add(virtualBufferCache);
        }

        this.ioScheduler = new AsynchronousScheduler(threadFactory, new IIoOperationFailedCallback() {
            @Override
            public void operationFailed(ILSMIOOperation operation, Throwable t) {
                LOGGER.error("Operation {} failed", operation, t);
            }

            @Override
            public void schedulerFailed(ILSMIOOperationScheduler scheduler, Throwable failure) {
                ExitUtil.exit(ExitUtil.EC_IO_SCHEDULER_FAILED);
            }
        });

        lsmtree = LSMBTreeUtil.createLSMTree(ioManager, virtualBufferCaches, file, bufferCache, typeTraits,
                cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, new NoMergePolicy(),
                new ThreadCountingTracker(), ioScheduler, NoOpIOOperationCallbackFactory.INSTANCE,
                NoOpPageWriteCallbackFactory.INSTANCE, true, null, null, null, null, true,
                TestStorageManagerComponentHolder.getMetadataPageManagerFactory(), false, ITracer.NONE,
                NoOpCompressorDecompressorFactory.INSTANCE);
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
            lsmTreeAccessor = lsmTree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                            lsmTreeAccessor.insert(batch.get(j));
                        } catch (Exception e) {
                            throw e;
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
