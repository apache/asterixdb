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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class InMemorySortRunner implements IExperimentRunner {
    public class TupleComparator implements Comparator<ITupleReference> {
        private final MultiComparator cmp;

        public TupleComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(ITupleReference o1, ITupleReference o2) {
            return cmp.compare(o1, o2);
        }
    }
    
    private final TupleComparator tupleCmp;
    private final int numBatches;
    private final int batchSize;
    private final int tupleSize;
    private final ITypeTraits[] typeTraits;
    
    private final TypeAwareTupleWriterFactory tupleWriterFactory;
    private final TypeAwareTupleWriter tupleWriter;
    private final ArrayList<TypeAwareTupleReference> tuples;        
    private final ByteBuffer tupleBuf; 
    
    public InMemorySortRunner(int numBatches, int batchSize, int tupleSize, ITypeTraits[] typeTraits, MultiComparator cmp) {
        this.numBatches = numBatches;
        this.tupleSize = tupleSize;
        this.batchSize = batchSize;
        this.typeTraits = typeTraits;
        tupleCmp = new TupleComparator(cmp);
        tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        tupleWriter = (TypeAwareTupleWriter) tupleWriterFactory.createTupleWriter();
        int numTuples = numBatches * batchSize;
        tuples = new ArrayList<TypeAwareTupleReference>();
        tupleBuf = ByteBuffer.allocate(numTuples * tupleSize);
        for (int i = 0; i < numTuples; i++) {
            tuples.add((TypeAwareTupleReference) tupleWriter.createTupleReference());
        }
    }
    
    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws InterruptedException {
        // Wait until the tupleBatchQueue is completely full.
        while (dataGen.tupleBatchQueue.remainingCapacity() != 0) {
            Thread.sleep(10);
        }
        
        long start = System.currentTimeMillis();
        int tupleIndex = 0;
        for (int i = 0; i < numBatches; i++) {
            TupleBatch batch = dataGen.tupleBatchQueue.take();
            for (int j = 0; j < batch.size(); j++) {
                // Copy the tuple to the buffer and set the pre-created tuple ref.                        
                tupleWriter.writeTuple(batch.get(j), tupleBuf.array(), tupleIndex * tupleSize);
                tuples.get(tupleIndex).resetByTupleOffset(tupleBuf, tupleIndex * tupleSize);
                tupleIndex++;
            }
        }
        // Perform the sort.        
        Collections.sort(tuples, tupleCmp);
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

        public SkipListThread(DataGenThread dataGen, ConcurrentSkipListSet<ITupleReference> skipList, int numBatches, int batchSize) {
            this.dataGen = dataGen;
            this.numBatches = numBatches;
            this.skipList = skipList;
            tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
            tupleWriter = (TypeAwareTupleWriter) tupleWriterFactory.createTupleWriter();
            int numTuples = numBatches * batchSize;
            tuples = new TypeAwareTupleReference[numTuples];
            tupleBuf = ByteBuffer.allocate(numTuples * tupleSize);
            for (int i = 0; i < numTuples; i++) {
                tuples[i] = (TypeAwareTupleReference) tupleWriter.createTupleReference();
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
                        tuples[tupleIndex].resetByTupleOffset(tupleBuf, tupleIndex * tupleSize);
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
