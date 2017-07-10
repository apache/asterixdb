/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.metadata.lock;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.om.base.AMutableInt32;

public class DatasetLock implements IMetadataLock {

    private final String key;
    private final ReentrantReadWriteLock dsLock;
    private final ReentrantReadWriteLock dsModifyLock;
    private final AMutableInt32 indexBuildCounter;

    public DatasetLock(String key) {
        this.key = key;
        dsLock = new ReentrantReadWriteLock(true);
        dsModifyLock = new ReentrantReadWriteLock(true);
        indexBuildCounter = new AMutableInt32(0);
    }

    private void acquireReadLock() {
        // query
        // build index
        // insert
        dsLock.readLock().lock();
    }

    private void releaseReadLock() {
        // query
        // build index
        // insert
        dsLock.readLock().unlock();
    }

    private void acquireWriteLock() {
        // create ds
        // delete ds
        // drop index
        dsLock.writeLock().lock();
    }

    private void releaseWriteLock() {
        // create ds
        // delete ds
        // drop index
        dsLock.writeLock().unlock();
    }

    private void acquireReadModifyLock() {
        // insert
        dsModifyLock.readLock().lock();
    }

    private void releaseReadModifyLock() {
        // insert
        dsModifyLock.readLock().unlock();
    }

    private void acquireWriteModifyLock() {
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

    private void releaseWriteModifyLock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getIntegerValue() == 1) {
                dsModifyLock.writeLock().unlock();
            }
            indexBuildCounter.setValue(indexBuildCounter.getIntegerValue() - 1);
        }
    }

    private void acquireRefreshLock() {
        // Refresh External Dataset statement
        dsModifyLock.writeLock().lock();
    }

    private void releaseRefreshLock() {
        // Refresh External Dataset statement
        dsModifyLock.writeLock().unlock();
    }

    @Override
    public void acquire(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                acquireReadLock();
                acquireWriteModifyLock();
                break;
            case MODIFY:
                acquireReadLock();
                acquireReadModifyLock();
                break;
            case REFRESH:
                acquireReadLock();
                acquireRefreshLock();
                break;
            case INDEX_DROP:
            case WRITE:
                acquireWriteLock();
                break;
            default:
                acquireReadLock();
                break;
        }
    }

    @Override
    public void release(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                releaseWriteModifyLock();
                releaseReadLock();
                break;
            case MODIFY:
                releaseReadModifyLock();
                releaseReadLock();
                break;
            case REFRESH:
                releaseRefreshLock();
                releaseReadLock();
                break;
            case INDEX_DROP:
            case WRITE:
                releaseWriteLock();
                break;
            default:
                releaseReadLock();
                break;
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DatasetLock)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        return Objects.equals(key, ((DatasetLock) o).key);
    }
}
