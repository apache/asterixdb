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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestLsmBtreeLocalResource extends LSMBTreeLocalResource {
    private static final long serialVersionUID = 1L;

    public TestLsmBtreeLocalResource(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate, boolean isPrimary, String path,
            IStorageManager storageManager, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, boolean durable) {
        super(typeTraits, cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, isPrimary, path,
                storageManager, mergePolicyFactory, mergePolicyProperties, filterTypeTraits, filterCmpFactories,
                btreeFields, filterFields, opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, durable,
                NoOpCompressorDecompressorFactory.INSTANCE);
    }

    protected TestLsmBtreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int[] bloomFilterKeyFields,
            double bloomFilterFalsePositiveRate, boolean isPrimary, int[] btreeFields) throws HyracksDataException {
        super(registry, json, bloomFilterKeyFields, bloomFilterFalsePositiveRate, isPrimary, btreeFields,
                NoOpCompressorDecompressorFactory.INSTANCE);
    }

    @Override
    public ILSMIndex createInstance(INCServiceContext serviceCtx) throws HyracksDataException {
        IIOManager ioManager = serviceCtx.getIoManager();
        FileReference file = ioManager.resolve(path);
        List<IVirtualBufferCache> vbcs = vbcProvider.getVirtualBufferCaches(serviceCtx, file);
        for (int i = 0; i < vbcs.size(); i++) {
            IVirtualBufferCache vbc = vbcs.get(i);
            if (!(vbc instanceof TestVirtualBufferCache)) {
                vbcs.remove(i);
                vbcs.add(i, new TestVirtualBufferCache(vbc));
            }
        }
        ioOpCallbackFactory.initialize(serviceCtx, this);
        pageWriteCallbackFactory.initialize(serviceCtx, this);
        return TestLsmBtreeUtil.createLSMTree(ioManager, vbcs, file, storageManager.getBufferCache(serviceCtx),
                typeTraits, cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, serviceCtx),
                opTrackerProvider.getOperationTracker(serviceCtx, this), ioSchedulerProvider.getIoScheduler(serviceCtx),
                ioOpCallbackFactory, pageWriteCallbackFactory, isPrimary, filterTypeTraits, filterCmpFactories,
                btreeFields, filterFields, durable, metadataPageManagerFactory, false, serviceCtx.getTracer());
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        appendToJson(jsonObject, registry);
        return jsonObject;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final int[] bloomFilterKeyFields = OBJECT_MAPPER.convertValue(json.get("bloomFilterKeyFields"), int[].class);
        final double bloomFilterFalsePositiveRate = json.get("bloomFilterFalsePositiveRate").asDouble();
        final boolean isPrimary = json.get("isPrimary").asBoolean();
        final int[] btreeFields = OBJECT_MAPPER.convertValue(json.get("btreeFields"), int[].class);
        return new TestLsmBtreeLocalResource(registry, json, bloomFilterKeyFields, bloomFilterFalsePositiveRate,
                isPrimary, btreeFields);
    }
}
