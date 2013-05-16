package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.util.concurrent.atomic.AtomicInteger;

public class DatasetIdFactory {
    private static AtomicInteger id = new AtomicInteger();
    private static boolean isInitialized = false; 
    
    public static boolean isInitialized() {
        return isInitialized;
    }
    
    public static void initialize(int initialId) {
    	id.set(initialId);
    	isInitialized = true;
    }

    public static int generateDatasetId() {
        return id.incrementAndGet();
    }
    
    public static int getMostRecentDatasetId() {
        return id.get();
    }
}