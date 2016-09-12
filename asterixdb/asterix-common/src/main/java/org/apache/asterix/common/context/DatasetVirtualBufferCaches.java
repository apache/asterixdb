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
package org.apache.asterix.common.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.MultitenantVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.IResourceMemoryManager;
import org.apache.hyracks.storage.common.buffercache.ResourceHeapBufferAllocator;

public class DatasetVirtualBufferCaches {
    private final int datasetID;
    private final AsterixStorageProperties storageProperties;
    private final int firstAvilableUserDatasetID;
    private final int numPartitions;
    private final Map<Integer, List<IVirtualBufferCache>> ioDeviceVirtualBufferCaches = new HashMap<>();

    public DatasetVirtualBufferCaches(int datasetID, AsterixStorageProperties storageProperties,
            int firstAvilableUserDatasetID, int numPartitions) {
        this.datasetID = datasetID;
        this.storageProperties = storageProperties;
        this.firstAvilableUserDatasetID = firstAvilableUserDatasetID;
        this.numPartitions = numPartitions;
    }

    public List<IVirtualBufferCache> initializeVirtualBufferCaches(IResourceMemoryManager memoryManager,
            int ioDeviceNum) {
        int numPages = datasetID < firstAvilableUserDatasetID
                ? storageProperties.getMetadataMemoryComponentNumPages()
                : storageProperties.getMemoryComponentNumPages();
        List<IVirtualBufferCache> vbcs = new ArrayList<>();
        for (int i = 0; i < storageProperties.getMemoryComponentsNum(); i++) {
            MultitenantVirtualBufferCache vbc = new MultitenantVirtualBufferCache(
                    new VirtualBufferCache(
                            new ResourceHeapBufferAllocator(memoryManager,
                                    Integer.toString(datasetID)),
                            storageProperties.getMemoryComponentPageSize(),
                            numPages / storageProperties.getMemoryComponentsNum() / numPartitions));
            vbcs.add(vbc);
        }
        ioDeviceVirtualBufferCaches.put(ioDeviceNum, vbcs);
        return vbcs;
    }

    public List<IVirtualBufferCache> getVirtualBufferCaches(IResourceMemoryManager memoryManager, int ioDeviceNum) {
        synchronized (ioDeviceVirtualBufferCaches) {
            List<IVirtualBufferCache> vbcs = ioDeviceVirtualBufferCaches.get(ioDeviceNum);
            if (vbcs == null) {
                vbcs = initializeVirtualBufferCaches(memoryManager, ioDeviceNum);
            }
            return vbcs;
        }
    }

    public long getTotalSize() {
        int numPages = datasetID < firstAvilableUserDatasetID
                ? storageProperties.getMetadataMemoryComponentNumPages()
                : storageProperties.getMemoryComponentNumPages();
        return storageProperties.getMemoryComponentPageSize() * ((long) numPages);
    }
}
