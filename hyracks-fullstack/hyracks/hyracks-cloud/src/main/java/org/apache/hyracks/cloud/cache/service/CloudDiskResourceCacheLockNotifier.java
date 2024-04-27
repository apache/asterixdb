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

import org.apache.hyracks.cloud.cache.unit.DatasetUnit;
import org.apache.hyracks.cloud.cache.unit.IndexUnit;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.disk.IDiskResourceCacheLockNotifier;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

// TODO locking should be revised
public final class CloudDiskResourceCacheLockNotifier implements IDiskResourceCacheLockNotifier {
    private final Int2ObjectMap<DatasetUnit> datasets;

    public CloudDiskResourceCacheLockNotifier() {
        datasets = Int2ObjectMaps.synchronize(new Int2ObjectOpenHashMap<>());
    }

    @Override
    public void onRegister(int datasetId, LocalResource localResource, IIndex index) {
        ILSMIndex lsmIndex = (ILSMIndex) index;
        if (lsmIndex.getDiskCacheManager().isSweepable()) {
            DatasetUnit datasetUnit = datasets.computeIfAbsent(datasetId, DatasetUnit::new);
            datasetUnit.addIndex(localResource.getId(), lsmIndex);
        }
    }

    @Override
    public void onUnregister(int datasetId, long resourceId) {
        DatasetUnit datasetUnit = datasets.get(datasetId);
        if (datasetUnit != null && datasetUnit.dropIndex(resourceId)) {
            datasets.remove(datasetId);

            // TODO invalidate eviction plans if the disk is not pressured
        }
    }

    @Override
    public void onOpen(int datasetId, long resourceId) {
        DatasetUnit datasetUnit = datasets.get(datasetId);
        if (datasetUnit != null) {
            IndexUnit indexUnit = datasetUnit.getIndex(resourceId);
            if (indexUnit != null) {
                indexUnit.readLock();
            }
        }
    }

    @Override
    public void onClose(int datasetId, long resourceId) {
        DatasetUnit datasetUnit = datasets.get(datasetId);
        if (datasetUnit != null) {
            IndexUnit indexUnit = datasetUnit.getIndex(resourceId);
            if (indexUnit != null) {
                indexUnit.readUnlock();
            }
        }
    }

    Int2ObjectMap<DatasetUnit> getDatasets() {
        return datasets;
    }
}
