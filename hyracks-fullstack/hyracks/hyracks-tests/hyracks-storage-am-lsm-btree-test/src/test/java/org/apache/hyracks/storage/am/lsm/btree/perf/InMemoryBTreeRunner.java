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
import java.util.Date;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.datagen.TupleBatch;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;

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
            IBinaryComparatorFactory[] cmpFactories) throws HyracksDataException {
        this.numBatches = numBatches;
        TestStorageManagerComponentHolder.init(pageSize, numPages, numPages);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        file = ioManager.resolveAbsolutePath(fileName);
        init(pageSize, numPages, typeTraits, cmpFactories);
    }

    protected void init(int pageSize, int numPages, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories)
            throws HyracksDataException {
        bufferCache = new VirtualBufferCache(new HeapBufferAllocator(), pageSize, numPages);
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory = new BTreeTypeAwareTupleWriterFactory(typeTraits, false);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        IPageManager freePageManager = new VirtualFreePageManager(bufferCache);
        btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                typeTraits.length, file);
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
            indexAccessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        try {
                            indexAccessor.insert(batch.get(j));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
