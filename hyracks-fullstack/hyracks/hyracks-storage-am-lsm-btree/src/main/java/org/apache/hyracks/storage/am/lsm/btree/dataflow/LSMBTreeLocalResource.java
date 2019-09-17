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
package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.io.IOException;
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
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.ReflectionUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMBTreeLocalResource extends LsmResource {

    private static final long serialVersionUID = 1L;

    protected final int[] bloomFilterKeyFields;
    protected final double bloomFilterFalsePositiveRate;
    protected final boolean isPrimary;
    protected final int[] btreeFields;
    protected final ICompressorDecompressorFactory compressorDecompressorFactory;

    public LSMBTreeLocalResource(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, double bloomFilterFalsePositiveRate, boolean isPrimary, String path,
            IStorageManager storageManager, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, boolean durable,
            ICompressorDecompressorFactory compressorDecompressorFactory) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable);
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.isPrimary = isPrimary;
        this.btreeFields = btreeFields;
        this.compressorDecompressorFactory = compressorDecompressorFactory;
    }

    protected LSMBTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int[] bloomFilterKeyFields,
            double bloomFilterFalsePositiveRate, boolean isPrimary, int[] btreeFields,
            ICompressorDecompressorFactory compressorDecompressorFactory) throws HyracksDataException {
        super(registry, json);
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.isPrimary = isPrimary;
        this.btreeFields = btreeFields;
        this.compressorDecompressorFactory = compressorDecompressorFactory;
    }

    @Override
    public ILSMIndex createInstance(INCServiceContext serviceCtx) throws HyracksDataException {
        IIOManager ioManager = serviceCtx.getIoManager();
        FileReference file = ioManager.resolve(path);
        List<IVirtualBufferCache> vbcs = vbcProvider.getVirtualBufferCaches(serviceCtx, file);
        ioOpCallbackFactory.initialize(serviceCtx, this);
        pageWriteCallbackFactory.initialize(serviceCtx, this);
        //TODO: enable updateAwareness for secondary LSMBTree indexes
        boolean updateAware = false;
        return LSMBTreeUtil.createLSMTree(ioManager, vbcs, file, storageManager.getBufferCache(serviceCtx), typeTraits,
                cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, serviceCtx),
                opTrackerProvider.getOperationTracker(serviceCtx, this), ioSchedulerProvider.getIoScheduler(serviceCtx),
                ioOpCallbackFactory, pageWriteCallbackFactory, isPrimary, filterTypeTraits, filterCmpFactories,
                btreeFields, filterFields, durable, metadataPageManagerFactory, updateAware, serviceCtx.getTracer(),
                compressorDecompressorFactory);
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
        final JsonNode compressorDecompressorNode = json.get("compressorDecompressorFactory");
        final ICompressorDecompressorFactory compDecompFactory = (ICompressorDecompressorFactory) registry
                .deserializeOrDefault(compressorDecompressorNode, NoOpCompressorDecompressorFactory.class);
        return new LSMBTreeLocalResource(registry, json, bloomFilterKeyFields, bloomFilterFalsePositiveRate, isPrimary,
                btreeFields, compDecompFactory);
    }

    @Override
    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        super.appendToJson(json, registry);
        json.putPOJO("bloomFilterKeyFields", bloomFilterKeyFields);
        json.put("bloomFilterFalsePositiveRate", bloomFilterFalsePositiveRate);
        json.put("isPrimary", isPrimary);
        json.putPOJO("btreeFields", btreeFields);
        json.putPOJO("compressorDecompressorFactory", compressorDecompressorFactory.toJson(registry));
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // compat w/ 0.3.4
        if (compressorDecompressorFactory == null) {
            ReflectionUtils.writeField(this, "compressorDecompressorFactory",
                    NoOpCompressorDecompressorFactory.INSTANCE);
        }
    }

}
