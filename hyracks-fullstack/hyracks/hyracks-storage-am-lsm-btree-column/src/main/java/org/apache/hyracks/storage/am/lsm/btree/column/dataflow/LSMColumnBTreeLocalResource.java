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
package org.apache.hyracks.storage.am.lsm.btree.column.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.LSMColumnBTreeUtil;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.disk.IDiskCacheMonitoringService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMColumnBTreeLocalResource extends LSMBTreeLocalResource {
    private final IColumnManagerFactory columnManagerFactory;

    public LSMColumnBTreeLocalResource(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate, String path,
            IStorageManager storageManager, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] btreeFields, ILSMOperationTrackerFactory opTrackerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ICompressorDecompressorFactory compressorDecompressorFactory, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector, boolean isSecondaryNoIncrementalMaintenance,
            IColumnManagerFactory columnManagerFactory, boolean atomic) {
        super(typeTraits, cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, true, path, storageManager,
                mergePolicyFactory, mergePolicyProperties, null, null, btreeFields, null, opTrackerProvider,
                ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider,
                ioSchedulerProvider, true, compressorDecompressorFactory, true, nullTypeTraits, nullIntrospector,
                isSecondaryNoIncrementalMaintenance, atomic);
        this.columnManagerFactory = columnManagerFactory;
    }

    private LSMColumnBTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int[] bloomFilterKeyFields,
            double bloomFilterFalsePositiveRate, boolean isPrimary, int[] btreeFields,
            ICompressorDecompressorFactory compressorDecompressorFactory, boolean hasBloomFilter,
            boolean isSecondaryNoIncrementalMaintenance, IColumnManagerFactory columnManagerFactory, boolean atomic)
            throws HyracksDataException {
        super(registry, json, bloomFilterKeyFields, bloomFilterFalsePositiveRate, isPrimary, btreeFields,
                compressorDecompressorFactory, hasBloomFilter, isSecondaryNoIncrementalMaintenance, atomic);
        this.columnManagerFactory = columnManagerFactory;
    }

    @Override
    public ILSMIndex createInstance(INCServiceContext serviceCtx) throws HyracksDataException {
        IIOManager ioManager = storageManager.getIoManager(serviceCtx);
        FileReference file = ioManager.resolve(path);
        List<IVirtualBufferCache> vbcs = vbcProvider.getVirtualBufferCaches(serviceCtx, file);
        ioOpCallbackFactory.initialize(serviceCtx, this);
        pageWriteCallbackFactory.initialize(serviceCtx, this);
        IDiskCacheMonitoringService diskCacheService = storageManager.getDiskCacheMonitoringService(serviceCtx);
        return LSMColumnBTreeUtil.createLSMTree(ioManager, vbcs, file, storageManager.getBufferCache(serviceCtx),
                typeTraits, cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, serviceCtx),
                opTrackerProvider.getOperationTracker(serviceCtx, this), ioSchedulerProvider.getIoScheduler(serviceCtx),
                ioOpCallbackFactory, pageWriteCallbackFactory, btreeFields, metadataPageManagerFactory, false,
                serviceCtx.getTracer(), compressorDecompressorFactory, nullTypeTraits, nullIntrospector,
                columnManagerFactory, atomic, diskCacheService);
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        int[] bloomFilterKeyFields = OBJECT_MAPPER.convertValue(json.get("bloomFilterKeyFields"), int[].class);
        double bloomFilterFalsePositiveRate = json.get("bloomFilterFalsePositiveRate").asDouble();
        boolean isPrimary = json.get("isPrimary").asBoolean();
        boolean hasBloomFilter = getOrDefaultHasBloomFilter(json, isPrimary);
        int[] btreeFields = OBJECT_MAPPER.convertValue(json.get("btreeFields"), int[].class);
        JsonNode compressorDecompressorNode = json.get("compressorDecompressorFactory");
        ICompressorDecompressorFactory compDecompFactory = (ICompressorDecompressorFactory) registry
                .deserializeOrDefault(compressorDecompressorNode, NoOpCompressorDecompressorFactory.class);
        JsonNode columnManagerFactoryNode = json.get("columnManagerFactory");
        boolean isSecondaryNoIncrementalMaintenance =
                getOrDefaultBoolean(json, "isSecondaryNoIncrementalMaintenance", false);
        boolean atomic = getOrDefaultBoolean(json, "atomic", false);
        IColumnManagerFactory columnManagerFactory =
                (IColumnManagerFactory) registry.deserialize(columnManagerFactoryNode);
        return new LSMColumnBTreeLocalResource(registry, json, bloomFilterKeyFields, bloomFilterFalsePositiveRate,
                isPrimary, btreeFields, compDecompFactory, hasBloomFilter, isSecondaryNoIncrementalMaintenance,
                columnManagerFactory, atomic);
    }

    @Override
    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        super.appendToJson(json, registry);
        json.putPOJO("columnManagerFactory", columnManagerFactory.toJson(registry));
    }
}
