package edu.uci.ics.asterix.tools.external.data;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class GULongIDGenerator {

    private final int partition;
    private final long baseValue;
    private final AtomicLong nextValue;

    public GULongIDGenerator(int partition, byte seed) {
        this.partition = partition;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(seed);
        buffer.put((byte) partition);
        buffer.putInt(0);
        buffer.putShort((short) 0);
        buffer.flip();
        this.baseValue = new Long(buffer.getLong());
        this.nextValue = new AtomicLong(baseValue);
    }

    public long getNextULong() {
        return nextValue.incrementAndGet();
    }
    
    public int getPartition(){
        return partition;
    }
 
}
