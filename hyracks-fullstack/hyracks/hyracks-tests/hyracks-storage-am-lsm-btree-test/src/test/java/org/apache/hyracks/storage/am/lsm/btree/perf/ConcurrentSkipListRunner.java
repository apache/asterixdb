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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.datagen.TupleBatch;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.common.MultiComparator;

public class ConcurrentSkipListRunner implements IExperimentRunner {
    public class TupleComparator implements Comparator<ITupleReference> {
        private final MultiComparator cmp;

        public TupleComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(ITupleReference o1, ITupleReference o2) {
            try {
                return cmp.compare(o1, o2);
            } catch (HyracksDataException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    private final TupleComparator tupleCmp;
    private final int numBatches;
    private final int batchSize;
    private final int tupleSize;
    private final ITypeTraits[] typeTraits;

    public ConcurrentSkipListRunner(int numBatches, int batchSize, int tupleSize, ITypeTraits[] typeTraits,
            MultiComparator cmp) {
        this.numBatches = numBatches;
        this.tupleSize = tupleSize;
        this.batchSize = batchSize;
        this.typeTraits = typeTraits;
        tupleCmp = new TupleComparator(cmp);
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws InterruptedException {
        ConcurrentSkipListSet<ITupleReference> skipList = new ConcurrentSkipListSet<ITupleReference>(tupleCmp);
        SkipListThread[] threads = new SkipListThread[numThreads];
        int threadNumBatches = numBatches / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new SkipListThread(dataGen, skipList, threadNumBatches, batchSize);
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
    }

    public void reset() throws Exception {
    }

    public class SkipListThread extends Thread {
        private final DataGenThread dataGen;
        private final ConcurrentSkipListSet<ITupleReference> skipList;
        private final int numBatches;
        public final TypeAwareTupleWriterFactory tupleWriterFactory;
        public final TypeAwareTupleWriter tupleWriter;
        public final TypeAwareTupleReference[] tuples;
        public final ByteBuffer tupleBuf;

        public SkipListThread(DataGenThread dataGen, ConcurrentSkipListSet<ITupleReference> skipList, int numBatches,
                int batchSize) {
            this.dataGen = dataGen;
            this.numBatches = numBatches;
            this.skipList = skipList;
            tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
            tupleWriter = tupleWriterFactory.createTupleWriter();
            int numTuples = numBatches * batchSize;
            tuples = new TypeAwareTupleReference[numTuples];
            tupleBuf = ByteBuffer.allocate(numTuples * tupleSize);
            for (int i = 0; i < numTuples; i++) {
                tuples[i] = tupleWriter.createTupleReference();
            }
        }

        @Override
        public void run() {
            int tupleIndex = 0;
            try {
                for (int i = 0; i < numBatches; i++) {
                    TupleBatch batch = dataGen.tupleBatchQueue.take();
                    for (int j = 0; j < batch.size(); j++) {
                        // Copy the tuple to the buffer and set the pre-created tuple ref.
                        tupleWriter.writeTuple(batch.get(j), tupleBuf.array(), tupleIndex * tupleSize);
                        tuples[tupleIndex].resetByTupleOffset(tupleBuf.array(), tupleIndex * tupleSize);
                        skipList.add(tuples[tupleIndex]);
                        tupleIndex++;
                    }
                }
            } catch (Exception e) {
                System.out.println(tupleIndex);
                e.printStackTrace();
            }
        }
    }
}
