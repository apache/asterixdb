package edu.uci.ics.hyracks.storage.am.common.util;

import java.util.concurrent.atomic.AtomicInteger;

public class DummyIndexIdGenerator {
    static private final AtomicInteger indexIdGenerator = new AtomicInteger();
    
    static public int generateIndexId() {
        return indexIdGenerator.getAndIncrement();
    }
    
    static public void resetIndexId(int newValue) {
        indexIdGenerator.set(newValue);
    }
}
