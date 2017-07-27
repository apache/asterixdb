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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.IMetadataLock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class DatasetLock implements IMetadataLock {

    private final String key;
    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock dsReadLock;
    private final ReentrantReadWriteLock dsModifyLock;
    private final MutableInt indexBuildCounter;

    public DatasetLock(String key) {
        this.key = key;
        lock = new ReentrantReadWriteLock(true);
        dsReadLock = new ReentrantReadWriteLock(true);
        dsModifyLock = new ReentrantReadWriteLock(true);
        indexBuildCounter = new MutableInt(0);
    }

    private void readLock() {
        // query
        // build index
        // insert
        lock.readLock().lock();
    }

    private void readUnlock() {
        // query
        // build index
        // insert
        lock.readLock().unlock();
    }

    private void writeLock() {
        // create ds
        // delete ds
        // drop index
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        // create ds
        // delete ds
        // drop index
        lock.writeLock().unlock();
    }

    private void readReadLock() {
        dsReadLock.readLock().lock();
    }

    private void modifyReadLock() {
        // insert
        dsModifyLock.readLock().lock();
    }

    private void modifyReadUnlock() {
        // insert
        dsModifyLock.readLock().unlock();
    }

    private void readReadUnlock() {
        dsReadLock.readLock().unlock();
    }

    private void readWriteUnlock() {
        dsReadLock.writeLock().unlock();
    }

    private void modifySharedWriteLock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getValue() > 0) {
                indexBuildCounter.setValue(indexBuildCounter.getValue() + 1);
            } else {
                dsModifyLock.writeLock().lock();
                indexBuildCounter.setValue(1);
            }
        }
    }

    private void modifySharedWriteUnlock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getValue() == 1) {
                dsModifyLock.writeLock().unlock();
            }
            indexBuildCounter.setValue(indexBuildCounter.getValue() - 1);
        }
    }

    private void modifyExclusiveWriteLock() {
        dsModifyLock.writeLock().lock();
    }

    private void modifyExclusiveWriteUnlock() {
        dsModifyLock.writeLock().unlock();
    }

    @Override
    public void upgrade(IMetadataLock.Mode from, IMetadataLock.Mode to) throws AlgebricksException {
        if (from == IMetadataLock.Mode.EXCLUSIVE_MODIFY && to == IMetadataLock.Mode.UPGRADED_WRITE) {
            dsReadLock.readLock().unlock();
            dsReadLock.writeLock().lock();
        } else {
            throw new MetadataException(ErrorCode.ILLEGAL_LOCK_UPGRADE_OPERATION, from, to);
        }
    }

    @Override
    public void downgrade(IMetadataLock.Mode from, IMetadataLock.Mode to) throws AlgebricksException {
        if (from == IMetadataLock.Mode.UPGRADED_WRITE && to == IMetadataLock.Mode.EXCLUSIVE_MODIFY) {
            dsReadLock.writeLock().unlock();
            dsReadLock.readLock().lock();
        } else {
            throw new MetadataException(ErrorCode.ILLEGAL_LOCK_DOWNGRADE_OPERATION, from, to);
        }
    }

    @Override
    public void lock(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                readLock();
                modifySharedWriteLock();
                break;
            case MODIFY:
                readLock();
                readReadLock();
                modifyReadLock();
                break;
            case EXCLUSIVE_MODIFY:
                readLock();
                readReadLock();
                modifyExclusiveWriteLock();
                break;
            case WRITE:
                writeLock();
                break;
            case READ:
                readLock();
                readReadLock();
                break;
            default:
                throw new IllegalStateException("locking mode " + mode + " is not supported");
        }
    }

    @Override
    public void unlock(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                modifySharedWriteUnlock();
                readUnlock();
                break;
            case MODIFY:
                modifyReadUnlock();
                readReadUnlock();
                readUnlock();
                break;
            case EXCLUSIVE_MODIFY:
                modifyExclusiveWriteUnlock();
                readReadUnlock();
                readUnlock();
                break;
            case WRITE:
                writeUnlock();
                break;
            case READ:
                readReadUnlock();
                readUnlock();
                break;
            case UPGRADED_WRITE:
                readWriteUnlock();
                modifyExclusiveWriteUnlock();
                readUnlock();
                break;
            default:
                throw new IllegalStateException("unlocking mode " + mode + " is not supported");
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

    @Override
    public String toString() {
        return key;
    }
}
