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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MemoryComponentMetadata implements IComponentMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<org.apache.commons.lang3.tuple.Pair<IValueReference, ArrayBackedValueStorage>> store =
            new ArrayList<>();

    /**
     * Note: for memory metadata, it is expected that the key will be constant
     *
     * @throws HyracksDataException
     */
    @Override
    public void put(IValueReference key, IValueReference value) throws HyracksDataException {
        lock.writeLock().lock();
        try {
            ArrayBackedValueStorage stored = get(key);
            if (stored == null) {
                stored = new ArrayBackedValueStorage();
                store.add(Pair.of(key, stored));
            }
            stored.assign(value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Note: for memory metadata, it is expected that the key will be constant
     *
     * @throws HyracksDataException
     */
    @Override
    public void get(IValueReference key, ArrayBackedValueStorage value) throws HyracksDataException {
        lock.readLock().lock();
        try {
            value.reset();
            ArrayBackedValueStorage stored = get(key);
            if (stored != null) {
                value.append(stored);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private ArrayBackedValueStorage get(IValueReference key) {
        lock.readLock().lock();
        try {
            for (Pair<IValueReference, ArrayBackedValueStorage> pair : store) {
                if (pair.getKey().equals(key)) {
                    return pair.getValue();
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void copy(IMetadataPageManager mdpManager) throws HyracksDataException {
        lock.readLock().lock();
        try {
            LOGGER.trace("Copying Metadata into a different component");
            ITreeIndexMetadataFrame frame = mdpManager.createMetadataFrame();
            for (Pair<IValueReference, ArrayBackedValueStorage> pair : store) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Copying " + pair.getKey() + " : " + pair.getValue().getLength() + " bytes");
                }
                mdpManager.put(frame, pair.getKey(), pair.getValue());
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void copy(DiskComponentMetadata metadata) throws HyracksDataException {
        lock.readLock().lock();
        try {
            metadata.put(this);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void reset() {
        lock.writeLock().lock();
        try {
            store.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
