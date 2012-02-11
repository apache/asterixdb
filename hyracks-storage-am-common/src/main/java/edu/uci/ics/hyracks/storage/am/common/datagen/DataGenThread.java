package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;

/**
 * Quick & dirty data generator for performance testing. 
 *
 */
public class DataGenThread extends Thread {
    public final BlockingQueue<TupleBatch> tupleBatchQueue;
    private final int maxNumBatches;
    private final int maxOutstandingBatches;        
    private int numBatches = 0;
    private final Random rnd;
    
    // maxOutstandingBatches pre-created tuple-batches for populating the queue.
    private TupleBatch[] tupleBatches;
    private int ringPos;
    
    public DataGenThread(int numConsumers, int maxNumBatches, int batchSize, ISerializerDeserializer[] fieldSerdes, int payloadSize, int rndSeed, int maxOutstandingBatches, boolean sorted) {
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
    
    @Override
    public void run() {
        while(numBatches < maxNumBatches) {
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
