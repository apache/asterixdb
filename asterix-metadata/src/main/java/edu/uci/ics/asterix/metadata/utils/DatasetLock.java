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
