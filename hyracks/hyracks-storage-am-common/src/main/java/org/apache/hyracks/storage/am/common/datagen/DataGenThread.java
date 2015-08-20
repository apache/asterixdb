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

package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;

/**
 * Quick & dirty data generator for multi-thread testing.
 */
@SuppressWarnings("rawtypes")
public class DataGenThread extends Thread {
    public final BlockingQueue<TupleBatch> tupleBatchQueue;
    private final int maxNumBatches;
    private final int maxOutstandingBatches;
    private int numBatches = 0;
    private final Random rnd;

    // maxOutstandingBatches pre-created tuple-batches for populating the queue.
    private TupleBatch[] tupleBatches;
    private int ringPos;

    public DataGenThread(int numConsumers, int maxNumBatches, int batchSize, ISerializerDeserializer[] fieldSerdes,
            int payloadSize, int rndSeed, int maxOutstandingBatches, boolean sorted) {
        this.maxNumBatches = maxNumBatches;
        this.maxOutstandingBatches = maxOutstandingBatches;
        rnd = new Random(rndSeed);
        tupleBatches = new TupleBatch[maxOutstandingBatches];
        IFieldValueGenerator[] fieldGens = DataGenUtils.getFieldGensFromSerdes(fieldSerdes, rnd, sorted);
        for (int i = 0; i < maxOutstandingBatches; i++) {
            tupleBatches[i] = new TupleBatch(batchSize, fieldGens, fieldSerdes, payloadSize);
        }
        tupleBatchQueue = new LinkedBlockingQueue<TupleBatch>(maxOutstandingBatches);
        ringPos = 0;
    }

    public DataGenThread(int numConsumers, int maxNumBatches, int batchSize, ISerializerDeserializer[] fieldSerdes,
            IFieldValueGenerator[] fieldGens, int rndSeed, int maxOutstandingBatches) {
        this.maxNumBatches = maxNumBatches;
        this.maxOutstandingBatches = maxOutstandingBatches;
        rnd = new Random(rndSeed);
        tupleBatches = new TupleBatch[maxOutstandingBatches];
        for (int i = 0; i < maxOutstandingBatches; i++) {
            tupleBatches[i] = new TupleBatch(batchSize, fieldGens, fieldSerdes, 0);
        }
        tupleBatchQueue = new LinkedBlockingQueue<TupleBatch>(maxOutstandingBatches);
        ringPos = 0;
    }

    @Override
    public void run() {
        while (numBatches < maxNumBatches) {
            boolean added = false;
            try {
                if (tupleBatches[ringPos].inUse.compareAndSet(false, true)) {
                    tupleBatches[ringPos].generate();
                    tupleBatchQueue.put(tupleBatches[ringPos]);
                    added = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (added) {
                numBatches++;
                ringPos++;
                if (ringPos >= maxOutstandingBatches) {
                    ringPos = 0;
                }
            }
        }
    }

    public TupleBatch getBatch() throws InterruptedException {
        return tupleBatchQueue.take();
    }

    public void releaseBatch(TupleBatch batch) {
        batch.inUse.set(false);
    }
}
