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
package org.apache.hyracks.cloud.cache.service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hyracks.cloud.cache.unit.AbstractIndexUnit;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.cache.unit.UnsweepableIndexUnit;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.disk.IDiskResourceCacheLockNotifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public final class CloudDiskResourceCacheLockNotifier implements IDiskResourceCacheLockNotifier {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IEvictableLocalResourceFilter filter;
    private final Long2ObjectMap<LocalResource> inactiveResources;
    private final Long2ObjectMap<UnsweepableIndexUnit> unsweepableIndexes;
    private final Long2ObjectMap<SweepableIndexUnit> sweepableIndexes;
    private final ReentrantReadWriteLock evictionLock;

    public CloudDiskResourceCacheLockNotifier(IEvictableLocalResourceFilter filter) {
        this.filter = filter;
        inactiveResources = Long2ObjectMaps.synchronize(new Long2ObjectOpenHashMap<>());
        unsweepableIndexes = Long2ObjectMaps.synchronize(new Long2ObjectOpenHashMap<>());
        sweepableIndexes = Long2ObjectMaps.synchronize(new Long2ObjectOpenHashMap<>());
        evictionLock = new ReentrantReadWriteLock();
    }

    @Override
    public void onRegister(LocalResource localResource, IIndex index) {
        ILSMIndex lsmIndex = (ILSMIndex) index;
        evictionLock.readLock().lock();
        try {
            if (filter.accept(localResource)) {
                long resourceId = localResource.getId();
                if (lsmIndex.getDiskCacheManager().isSweepable()) {
                    sweepableIndexes.put(resourceId, new SweepableIndexUnit(localResource, lsmIndex));
                } else {
                    unsweepableIndexes.put(resourceId, new UnsweepableIndexUnit(localResource));
                }
                inactiveResources.remove(localResource.getId());
            }
        } finally {
            evictionLock.readLock().unlock();
        }
    }

    @Override
    public void onUnregister(long resourceId) {
        evictionLock.readLock().lock();
        try {
            AbstractIndexUnit indexUnit = removeUnit(resourceId);
            if (indexUnit != null) {
                indexUnit.drop();
            } else {
                inactiveResources.remove(resourceId);
            }
        } finally {
            evictionLock.readLock().unlock();
        }
    }

    @Override
    public void onOpen(long resourceId) {
        evictionLock.readLock().lock();
        try {
            AbstractIndexUnit indexUnit = getUnit(resourceId);
            if (indexUnit == null) {
                // Metadata resource
                return;
            }
            indexUnit.open();
        } finally {
            evictionLock.readLock().unlock();
        }
    }

    @Override
    public void onClose(long resourceId) {
        evictionLock.readLock().lock();
        try {
            AbstractIndexUnit indexUnit = getUnit(resourceId);
            if (indexUnit == null) {
                // Metadata resource
                return;
            }
            indexUnit.close();
        } finally {
            evictionLock.readLock().unlock();
        }
    }

    private AbstractIndexUnit getUnit(long resourceId) {
        AbstractIndexUnit indexUnit = sweepableIndexes.get(resourceId);
        if (indexUnit == null) {
            indexUnit = unsweepableIndexes.get(resourceId);
        }
        return indexUnit;
    }

    private AbstractIndexUnit removeUnit(long resourceId) {
        AbstractIndexUnit indexUnit = sweepableIndexes.remove(resourceId);
        if (indexUnit == null) {
            indexUnit = unsweepableIndexes.remove(resourceId);
        }
        return indexUnit;
    }

    ReentrantReadWriteLock getEvictionLock() {
        return evictionLock;
    }

    void reportLocalResources(Map<Long, LocalResource> localResources) {
        inactiveResources.clear();
        // First check whatever we had already
        for (LocalResource lr : localResources.values()) {
            if (!filter.accept(lr) || unsweepableIndexes.containsKey(lr.getId())
                    || sweepableIndexes.containsKey(lr.getId())) {
                // We already have this resource
                continue;
            }

            // Probably a new resource or an old resource that wasn't registered before
            inactiveResources.put(lr.getId(), lr);
        }

        removeUnassignedResources(unsweepableIndexes, localResources);
        removeUnassignedResources(sweepableIndexes, localResources);

        LOGGER.info("Retained active {unsweepable: {}, sweepable: {}} and inactive: {}", unsweepableIndexes,
                sweepableIndexes, inactiveResources.values().stream()
                        .map(x -> "(id: " + x.getId() + ",  path: " + x.getPath() + ")").toList());
    }

    private void removeUnassignedResources(Long2ObjectMap<?> indexes, Map<Long, LocalResource> localResources) {
        indexes.long2ObjectEntrySet().removeIf(x -> !localResources.containsKey(x.getLongKey()));
    }

    Collection<LocalResource> getInactiveResources() {
        return inactiveResources.values();
    }

    Collection<UnsweepableIndexUnit> getUnsweepableIndexes() {
        return unsweepableIndexes.values();
    }

    void getSweepableIndexes(Collection<SweepableIndexUnit> indexes) {
        indexes.addAll(sweepableIndexes.values());
    }
}
