package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;

public class BTreeRegistry {
    
    private HashMap<Integer, BTree> map = new HashMap<Integer, BTree>();
    private Lock registryLock = new ReentrantLock();
    
    public BTree get(int fileId) {
        return map.get(fileId);        
    }
    
    // TODO: not very high concurrency, but good enough for now    
    public void lock() {
        registryLock.lock();
    }
    
    public void unlock() {
        registryLock.unlock();
    }
    
    public void register(int fileId, BTree btree) {
        map.put(fileId, btree);
    }
    
    public void unregister(int fileId) {
        map.remove(fileId);
    }        
}
