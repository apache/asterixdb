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
import org.apache.hyracks.api.util.InvokeUtil;

public class DatasetLock implements IMetadataLock {

    private final String key;
    // The lock
    private final ReentrantReadWriteLock lock;
    // Used for lock upgrade operation
    private final ReentrantReadWriteLock upgradeLock;
    // Used for exclusive modification
    private final ReentrantReadWriteLock modifyLock;
    // The two counters below are used to ensure mutual exclusivity between index builds and modifications
    // order of entry indexBuildCounter -> indexModifyCounter
    private final MutableInt indexBuildCounter;
    private final MutableInt dsModifyCounter;

    public DatasetLock(String key) {
        this.key = key;
        lock = new ReentrantReadWriteLock(true);
        upgradeLock = new ReentrantReadWriteLock(true);
        modifyLock = new ReentrantReadWriteLock(true);
        indexBuildCounter = new MutableInt(0);
        dsModifyCounter = new MutableInt(0);
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

    private void upgradeReadLock() {
        upgradeLock.readLock().lock();
    }

    private void modifyReadLock() {
        // insert
        modifyLock.readLock().lock();
        incrementModifyCounter();
    }

    private void incrementModifyCounter() {
        InvokeUtil.doUninterruptibly(() -> {
            synchronized (indexBuildCounter) {
                while (indexBuildCounter.getValue() > 0) {
                    indexBuildCounter.wait();
                }
                synchronized (dsModifyCounter) {
                    dsModifyCounter.increment();
                }
            }
        });
    }

    private void decrementModifyCounter() {
        synchronized (indexBuildCounter) {
            synchronized (dsModifyCounter) {
                if (dsModifyCounter.decrementAndGet() == 0) {
                    indexBuildCounter.notifyAll();
                }
            }
        }
    }

    private void modifyReadUnlock() {
        // insert
        decrementModifyCounter();
        modifyLock.readLock().unlock();
    }

    private void upgradeReadUnlock() {
        upgradeLock.readLock().unlock();
    }

    private void upgradeWriteUnlock() {
        upgradeLock.writeLock().unlock();
    }

    private void buildIndexLock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.getValue() > 0) {
                indexBuildCounter.increment();
            } else {
                InvokeUtil.doUninterruptibly(() -> {
                    while (true) {
                        synchronized (dsModifyCounter) {
                            if (dsModifyCounter.getValue() == 0) {
                                indexBuildCounter.increment();
                                return;
                            }
                        }
                        indexBuildCounter.wait();
                    }
                });
            }
        }
    }

    private void buildIndexUnlock() {
        // Build index statement
        synchronized (indexBuildCounter) {
            if (indexBuildCounter.decrementAndGet() == 0) {
                indexBuildCounter.notifyAll();
            }
        }
    }

    private void modifyWriteLock() {
        modifyLock.writeLock().lock();
        incrementModifyCounter();
    }

    private void modifyExclusiveWriteUnlock() {
        decrementModifyCounter();
        modifyLock.writeLock().unlock();
    }

    @Override
    public void upgrade(IMetadataLock.Mode from, IMetadataLock.Mode to) throws AlgebricksException {
        if (from == IMetadataLock.Mode.EXCLUSIVE_MODIFY && to == IMetadataLock.Mode.UPGRADED_WRITE) {
            upgradeLock.writeLock().lock();
        } else {
            throw new MetadataException(ErrorCode.ILLEGAL_LOCK_UPGRADE_OPERATION, from, to);
        }
    }

    @Override
    public void downgrade(IMetadataLock.Mode from, IMetadataLock.Mode to) throws AlgebricksException {
        if (from == IMetadataLock.Mode.UPGRADED_WRITE && to == IMetadataLock.Mode.EXCLUSIVE_MODIFY) {
            upgradeLock.writeLock().unlock();
        } else {
            throw new MetadataException(ErrorCode.ILLEGAL_LOCK_DOWNGRADE_OPERATION, from, to);
        }
    }

    @Override
    public void lock(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                readLock();
                buildIndexLock();
                break;
            case MODIFY:
                readLock();
                modifyReadLock();
                break;
            case EXCLUSIVE_MODIFY:
                readLock();
                modifyWriteLock();
                break;
            case WRITE:
                writeLock();
                break;
            case READ:
                readLock();
                upgradeReadLock();
                break;
            default:
                throw new IllegalStateException("locking mode " + mode + " is not supported");
        }
    }

    @Override
    public void unlock(IMetadataLock.Mode mode) {
        switch (mode) {
            case INDEX_BUILD:
                buildIndexUnlock();
                readUnlock();
                break;
            case MODIFY:
                modifyReadUnlock();
                readUnlock();
                break;
            case EXCLUSIVE_MODIFY:
                modifyExclusiveWriteUnlock();
                readUnlock();
                break;
            case WRITE:
                writeUnlock();
                break;
            case READ:
                upgradeReadUnlock();
                readUnlock();
                break;
            case UPGRADED_WRITE:
                upgradeWriteUnlock();
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
