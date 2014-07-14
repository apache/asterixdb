package edu.uci.ics.asterix.metadata.utils;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.asterix.om.base.AMutableInt32;

public class DatasetLock {

    private ReentrantReadWriteLock dsLock;
    private ReentrantReadWriteLock dsModifyLock;
    private AMutableInt32 indexBuildCounter;

    public DatasetLock() {
        dsLock = new ReentrantReadWriteLock(true);
        dsModifyLock = new ReentrantReadWriteLock(true);
        indexBuildCounter = new AMutableInt32(0);
    }

    public void acquireReadLock() {
        // query
        // build index
        // insert
        dsLock.readLock().lock();
    }

    public void releaseReadLock() {
        // query
        // build index
        // insert
        dsLock.readLock().unlock();
    }

    public void acquireWriteLock() {
        // create ds
        // delete ds
        // drop index
        dsLock.writeLock().lock();
    }

    public void releaseWriteLock() {
        // create ds
        // delete ds
        // drop index
        dsLock.writeLock().unlock();
    }

    public void acquireReadModifyLock() {
        // insert
        dsModifyLock.readLock().lock();
    }

    public void releaseReadModifyLock() {
        // insert
        dsModifyLock.readLock().unlock();
    }

    public void acquireWriteModifyLock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getIntegerValue() > 0) {
                indexBuildCounter.setValue(indexBuildCounter.getIntegerValue() + 1);
            } else {
                dsModifyLock.writeLock().lock();
                indexBuildCounter.setValue(1);
            }
        }
    }

    public void releaseWriteModifyLock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getIntegerValue() == 1) {
                dsModifyLock.writeLock().unlock();
            }
            indexBuildCounter.setValue(indexBuildCounter.getIntegerValue() - 1);
        }
    }

    public void acquireRefreshLock() {
        // Refresh External Dataset statement
        dsModifyLock.writeLock().lock();
    }

    public void releaseRefreshLock() {
        // Refresh External Dataset statement
        dsModifyLock.writeLock().unlock();
    }
}
