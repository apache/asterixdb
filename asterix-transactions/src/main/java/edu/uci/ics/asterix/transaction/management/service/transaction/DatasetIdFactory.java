package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.util.concurrent.atomic.AtomicInteger;

public class DatasetIdFactory {
    private static AtomicInteger id = new AtomicInteger();
    
    public static void initialize(int initialId) {
    	id.set(initialId);
    }

    public static int generateDatasetId() {
        return id.incrementAndGet();
    }
}