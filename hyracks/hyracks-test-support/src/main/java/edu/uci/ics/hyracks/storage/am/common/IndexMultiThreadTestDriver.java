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

package edu.uci.ics.hyracks.storage.am.common;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;

@SuppressWarnings("rawtypes")
public class IndexMultiThreadTestDriver {
    protected static final int RANDOM_SEED = 50;
    // Means no additional payload. Only the specified fields.
    protected static final int PAYLOAD_SIZE = 0;
    protected final TestOperationSelector opSelector;
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IIndex index;
    protected final IIndexTestWorkerFactory workerFactory;

    public IndexMultiThreadTestDriver(IIndex index, IIndexTestWorkerFactory workerFactory,
            ISerializerDeserializer[] fieldSerdes, TestOperation[] ops, double[] opProbs) {
        this.index = index;
        this.workerFactory = workerFactory;
        this.fieldSerdes = fieldSerdes;
        this.opSelector = new TestOperationSelector(ops, opProbs);
    }

    public void init() throws HyracksDataException {
        index.create();
        index.activate();
    }

    public long[] run(int numThreads, int numRepeats, int numOps, int batchSize) throws InterruptedException,
            TreeIndexException, HyracksDataException {
        int numBatches = numOps / batchSize;
        int threadNumBatches = numBatches / numThreads;
        if (threadNumBatches <= 0) {
            throw new TreeIndexException("Inconsistent parameters given. Need at least one batch per thread.");
        }
        long[] times = new long[numRepeats];
        for (int i = 0; i < numRepeats; i++) {
            DataGenThread dataGen = createDatagenThread(numThreads, numBatches, batchSize);
            dataGen.start();
            // Wait until the tupleBatchQueue is filled to capacity.
            while (dataGen.tupleBatchQueue.remainingCapacity() != 0 && dataGen.tupleBatchQueue.size() != numBatches) {
                Thread.sleep(10);
            }

            // Start worker threads.
            AbstractIndexTestWorker[] workers = new AbstractIndexTestWorker[numThreads];
            long start = System.currentTimeMillis();
            for (int j = 0; j < numThreads; j++) {
                workers[j] = workerFactory.create(dataGen, opSelector, index, threadNumBatches);
                workers[j].start();
            }
            // Join worker threads.
            for (int j = 0; j < numThreads; j++) {
                workers[j].join();
            }
            long end = System.currentTimeMillis();
            times[i] = end - start;
        }
        return times;
    }

    public void deinit() throws HyracksDataException {
        index.deactivate();
        index.destroy();
    }

    // To allow subclasses to override the data gen params.
    public DataGenThread createDatagenThread(int numThreads, int numBatches, int batchSize) {
        return new DataGenThread(numThreads, numBatches, batchSize, fieldSerdes, PAYLOAD_SIZE, RANDOM_SEED,
                2 * numThreads, false);
    }
}
