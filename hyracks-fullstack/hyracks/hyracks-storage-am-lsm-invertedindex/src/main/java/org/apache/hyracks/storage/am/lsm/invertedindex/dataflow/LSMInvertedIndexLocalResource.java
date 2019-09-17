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
package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.util.ArrayList;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMInvertedIndexLocalResource extends LsmResource {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final boolean isPartitioned;
    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;
    private final double bloomFilterFalsePositiveRate;

    public LSMInvertedIndexLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            boolean isPartitioned, int[] invertedIndexFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps, double bloomFilterFalsePositiveRate) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable);
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    private LSMInvertedIndexLocalResource(IPersistedResourceRegistry registry, JsonNode json,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, boolean isPartitioned, int[] invertedIndexFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps,
            double bloomFilterFalsePositiveRate) throws HyracksDataException {
        super(registry, json);
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    @Override
    public ILSMIndex createInstance(INCServiceContext serviceCtx) throws HyracksDataException {
        IIOManager ioManager = serviceCtx.getIoManager();
        FileReference file = ioManager.resolve(path);
        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(serviceCtx, file);
        IBufferCache bufferCache = storageManager.getBufferCache(serviceCtx);
        ILSMMergePolicy mergePolicy = mergePolicyFactory.createMergePolicy(mergePolicyProperties, serviceCtx);
        ILSMIOOperationScheduler ioScheduler = ioSchedulerProvider.getIoScheduler(serviceCtx);
        ioOpCallbackFactory.initialize(serviceCtx, this);
        pageWriteCallbackFactory.initialize(serviceCtx, this);
        if (isPartitioned) {
            return InvertedIndexUtils.createPartitionedLSMInvertedIndex(ioManager, virtualBufferCaches, typeTraits,
                    cmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, bufferCache,
                    file.getAbsolutePath(), bloomFilterFalsePositiveRate, mergePolicy,
                    opTrackerProvider.getOperationTracker(serviceCtx, this), ioScheduler, ioOpCallbackFactory,
                    pageWriteCallbackFactory, invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                    filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable,
                    metadataPageManagerFactory, serviceCtx.getTracer());
        } else {
            return InvertedIndexUtils.createLSMInvertedIndex(ioManager, virtualBufferCaches, typeTraits, cmpFactories,
                    tokenTypeTraits, tokenCmpFactories, tokenizerFactory, bufferCache, file.getAbsolutePath(),
                    bloomFilterFalsePositiveRate, mergePolicy, opTrackerProvider.getOperationTracker(serviceCtx, this),
                    ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, invertedIndexFields, filterTypeTraits,
                    filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                    invertedIndexFieldsForNonBulkLoadOps, durable, metadataPageManagerFactory, serviceCtx.getTracer());
        }
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        super.appendToJson(jsonObject, registry);
        final ArrayNode tokenTypeTraitsArray = OBJECT_MAPPER.createArrayNode();
        for (ITypeTraits tt : tokenTypeTraits) {
            tokenTypeTraitsArray.add(tt.toJson(registry));
        }
        jsonObject.set("tokenTypeTraits", tokenTypeTraitsArray);
        final ArrayNode tokenCmpFactoriesArray = OBJECT_MAPPER.createArrayNode();
        for (IBinaryComparatorFactory factory : tokenCmpFactories) {
            tokenCmpFactoriesArray.add(factory.toJson(registry));
        }
        jsonObject.set("tokenCmpFactories", tokenCmpFactoriesArray);
        jsonObject.set("tokenizerFactory", tokenizerFactory.toJson(registry));
        jsonObject.put("isPartitioned", isPartitioned);
        jsonObject.putPOJO("invertedIndexFields", invertedIndexFields);
        jsonObject.putPOJO("filterFieldsForNonBulkLoadOps", filterFieldsForNonBulkLoadOps);
        jsonObject.putPOJO("invertedIndexFieldsForNonBulkLoadOps", invertedIndexFieldsForNonBulkLoadOps);
        jsonObject.putPOJO("bloomFilterFalsePositiveRate", bloomFilterFalsePositiveRate);
        return jsonObject;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final List<ITypeTraits> tokenTypeTraitsList = new ArrayList<>();
        final ArrayNode jsonTokenTypeTraits = (ArrayNode) json.get("tokenTypeTraits");
        for (JsonNode tt : jsonTokenTypeTraits) {
            tokenTypeTraitsList.add((ITypeTraits) registry.deserialize(tt));
        }
        final ITypeTraits[] tokenTypeTraits = tokenTypeTraitsList.toArray(new ITypeTraits[0]);

        final List<IBinaryComparatorFactory> tokenCmpFactoriesList = new ArrayList<>();
        final ArrayNode jsontokenCmpFactories = (ArrayNode) json.get("tokenCmpFactories");
        for (JsonNode cf : jsontokenCmpFactories) {
            tokenCmpFactoriesList.add((IBinaryComparatorFactory) registry.deserialize(cf));
        }
        final IBinaryComparatorFactory[] tokenCmpFactories =
                tokenCmpFactoriesList.toArray(new IBinaryComparatorFactory[0]);
        final IBinaryTokenizerFactory tokenizerFactory =
                (IBinaryTokenizerFactory) registry.deserialize(json.get("tokenizerFactory"));
        final boolean isPartitioned = json.get("isPartitioned").asBoolean();
        final int[] invertedIndexFields = OBJECT_MAPPER.convertValue(json.get("invertedIndexFields"), int[].class);
        final int[] filterFieldsForNonBulkLoadOps =
                OBJECT_MAPPER.convertValue(json.get("filterFieldsForNonBulkLoadOps"), int[].class);
        final int[] invertedIndexFieldsForNonBulkLoadOps =
                OBJECT_MAPPER.convertValue(json.get("invertedIndexFieldsForNonBulkLoadOps"), int[].class);
        final double bloomFilterFalsePositiveRate = json.get("bloomFilterFalsePositiveRate").asDouble();
        return new LSMInvertedIndexLocalResource(registry, json, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                isPartitioned, invertedIndexFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                bloomFilterFalsePositiveRate);
    }
}
