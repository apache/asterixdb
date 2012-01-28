package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * Quick & dirty data generator for performance testing. 
 *
 */
public class DataGenThread extends Thread {
    public final BlockingQueue<TupleBatch> tupleBatchQueue;
    private final int maxNumBatches;
    private final int maxOutstandingBatches;        
    private int numBatches;
    private final boolean sorted;
    private final Random rnd;
    
    // maxOutstandingBatches pre-created tuple-batches for populating the queue.
    private TupleBatch[] tupleBatches;
    private int ringPos;
    
    public DataGenThread(int maxNumBatches, int batchSize, int maxOutstandingBatches, int numConsumers, ISerializerDeserializer[] fieldSerdes, int payloadSize, int rndSeed, boolean sorted) {
        this.maxNumBatches = maxNumBatches;
        this.maxOutstandingBatches = maxOutstandingBatches;
        this.sorted = sorted;
        rnd = new Random(rndSeed);
        tupleBatches = new TupleBatch[maxOutstandingBatches];
        IFieldValueGenerator[] fieldGens = new IFieldValueGenerator[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldGens[i] = getFieldGenFromSerde(fieldSerdes[i]);
        }
        for (int i = 0; i < maxOutstandingBatches; i++) {
            tupleBatches[i] = new TupleBatch(batchSize, fieldGens, fieldSerdes, payloadSize);
        }
        // make sure we don't overwrite tuples that are in use by consumers. 
        // -1 because we first generate a new tuple, and then try to put it into the queue.
        int capacity = Math.max(maxOutstandingBatches - numConsumers - 1, 1);
        tupleBatchQueue = new LinkedBlockingQueue<TupleBatch>(capacity);
        ringPos = 0;
    }
    
    @Override
    public void run() {
        while(numBatches < maxNumBatches) {
            try {
                tupleBatches[ringPos].generate();
                tupleBatchQueue.put(tupleBatches[ringPos]);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            numBatches++;
            ringPos++;
            if (ringPos >= maxOutstandingBatches) {
                ringPos = 0;
            }
        }
    }
    
    public IFieldValueGenerator getFieldGenFromSerde(ISerializerDeserializer serde) {
        if (serde instanceof IntegerSerializerDeserializer) {
            if (sorted) {
                return new SortedIntegerFieldValueGenerator();
            } else {
                return new IntegerFieldValueGenerator(rnd);
            }
        } else if (serde instanceof FloatSerializerDeserializer) {
            if (sorted) {
                return new SortedFloatFieldValueGenerator();
            } else {
                return new FloatFieldValueGenerator(rnd);
            }
        } else if (serde instanceof DoubleSerializerDeserializer) {
            if (sorted) {
                return new SortedDoubleFieldValueGenerator();
            } else {
                return new DoubleFieldValueGenerator(rnd);
            }
        }
        System.out.println("NULL");
        //if (serde instanceof Integer64SerializerDeserializer) {
        //    throw new UnsupportedOperationException("Binary comparator factory for Integer64 not implemented.");
        //}
        //if (serde instanceof FloatSerializerDeserializer) {
        //    return FloatBinaryComparatorFactory.INSTANCE;
        //}
        //if (serde instanceof DoubleSerializerDeserializer) {
        //    return DoubleBinaryComparatorFactory.INSTANCE;
        //}
        return null;
    }
}
