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
package org.apache.hyracks.cloud.cache.unit;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public final class DatasetUnit {
    private final int id;
    private final ReentrantReadWriteLock lock;
    /**
     * Maps resourceId to {@link IndexUnit}
     */
    private final Long2ObjectMap<IndexUnit> indexes;

    public DatasetUnit(int datasetId) {
        id = datasetId;
        lock = new ReentrantReadWriteLock();
        indexes = new Long2ObjectOpenHashMap<>();

    }

    public int getId() {
        return id;
    }

    public IndexUnit addIndex(long resourceId, ILSMIndex index) {
        writeLock();
        try {
            IndexUnit indexUnit = new IndexUnit(resourceId, index);
            indexes.put(resourceId, indexUnit);
            return indexUnit;
        } finally {
            writeUnlock();
        }
    }

    public boolean dropIndex(long resourceId) {
        IndexUnit indexUnit = indexes.remove(resourceId);
        // Signal that the index is being dropped so a sweeper thread does not sweep this index or stops sweeping
        indexUnit.setDropped();
        // Wait for the sweep operation (if running) before allowing the index to be dropped
        indexUnit.waitForSweep();
        return indexUnit.getIndex().isPrimaryIndex();
    }

    public IndexUnit getIndex(long resourceId) {
        readLock();
        try {
            return indexes.get(resourceId);
        } finally {
            readUnlock();
        }
    }

    /**
     * Return the current indexes
     *
     * @param indexUnits container used to return the current indexes
     */
    public void getIndexes(List<IndexUnit> indexUnits) {
        readLock();
        try {
            indexUnits.addAll(indexes.values());
        } finally {
            readUnlock();
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }
}
