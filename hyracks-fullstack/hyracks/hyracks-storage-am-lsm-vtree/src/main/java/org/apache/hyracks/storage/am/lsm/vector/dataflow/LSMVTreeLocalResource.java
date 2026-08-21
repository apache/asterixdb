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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVTreeUtils;
import org.apache.hyracks.storage.am.vector.api.IQuantizedResource;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IStorageManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMVTreeLocalResource extends LsmResource implements IQuantizedResource {

    private static final long serialVersionUID = 2L;

    // Persisted/wire key names for quantization parameters. Shared with QuantizedIndexCreate so
    // the producer and the persisted resource agree on the vocabulary.
    public static final String KEY_MIN_QUANTILE = "minQuantile";
    public static final String KEY_MAX_QUANTILE = "maxQuantile";
    public static final String KEY_ALPHA = "alpha";
    public static final String KEY_BITS = "bits";
    public static final String KEY_CONFIDENCE_INTERVAL = "confidenceInterval";
    public static final String KEY_SAMPLE_COUNT = "sampleCount";

    private static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    /** JSON key under which the vector accessor factory is persisted. */
    private static final String KEY_VECTOR_ACCESSOR_FACTORY = "vectorAccessorFactory";
    /** JSON key under which the distance-function factory is persisted. */
    private static final String KEY_DISTANCE_FUNCTION_FACTORY = "distanceFunctionFactory";
    /** JSON keys for the cross-pollination placement parameters (see {@link CrossPollinationConfig}). */
    private static final String KEY_CROSS_POLLINATION_M = "crossPollinationM";
    private static final String KEY_RNG_FACTOR = "rngFactor";
    private static final String KEY_EPSILON = "epsilon";

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final int[] filterFields;
    protected final boolean atomic;
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    protected final int numPrimaryKeyFields;
    protected final int numIncludeFields;
    protected final IVTreeDataTupleBuilderFactory dataTupleBuilderFactory;

    /**
     * Distance-function factory supplied at DDL time. Persisted via the resource registry so a
     * restarted index reconstructs the same distance implementation.
     */
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;

    /**
     * Cross-pollination placement config supplied at DDL time. Persisted to JSON so that incremental
     * insert/delete on a restarted index replicate into the same leaf clusters bulk-load used (M=1 =
     * legacy single-closest).
     */
    protected final CrossPollinationConfig crossPollination;

    // Quantization parameters (optional, set by QuantizedIndexCreate during index creation)
    protected Float confidenceInterval;
    protected Float minQuantile;
    protected Float maxQuantile;
    protected Float alpha;
    protected Integer bits;
    protected Integer sampleCount;

    public LSMVTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, Float confidenceInterval, Float minQuantile,
            Float maxQuantile, Float alpha, Integer bits, Integer sampleCount,
            CrossPollinationConfig crossPollination) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, nullTypeTraits,
                nullIntrospector);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.crossPollination = crossPollination != null ? crossPollination : CrossPollinationConfig.LEGACY;
        this.confidenceInterval = confidenceInterval;
        this.minQuantile = minQuantile;
        this.maxQuantile = maxQuantile;
        this.alpha = alpha;
        this.bits = bits;
        this.sampleCount = sampleCount;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleBuilderFactory = dataTupleBuilderFactory;
    }

    protected LSMVTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic, Float confidenceInterval, Float minQuantile,
            Float maxQuantile, Float alpha, Integer bits, Integer sampleCount,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination)
            throws HyracksDataException {
        super(registry, json);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.crossPollination = crossPollination != null ? crossPollination : CrossPollinationConfig.LEGACY;
        this.confidenceInterval = confidenceInterval;
        this.minQuantile = minQuantile;
        this.maxQuantile = maxQuantile;
        this.alpha = alpha;
        this.bits = bits;
        this.sampleCount = sampleCount;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleBuilderFactory = dataTupleBuilderFactory;
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        IIOManager ioManager = storageManager.getIoManager(ncServiceCtx);
        NCConfig storageConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
        FileReference fileRef = ioManager.resolve(path);

        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(ncServiceCtx, fileRef);
        ioOpCallbackFactory.initialize(ncServiceCtx, this);
        pageWriteCallbackFactory.initialize(ncServiceCtx, this);

        // Construction-time invariant: vectorAccessorFactory is always supplied — by
        // VTreeResourceFactoryProvider in production and by the test harness in tests, and round-
        // tripped through the resource registry on JSON read. A null here indicates a corrupted
        // resource (e.g., a JSON file with the {@link #KEY_VECTOR_ACCESSOR_FACTORY} key missing).
        if (vectorAccessorFactory == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "The vector accessor factory is missing from the resource at " + path);
        }
        // Distance-function factory is always supplied at DDL and round-tripped through the registry;
        // a null indicates a corrupted or unsupported-version resource.
        if (distanceFunctionFactory == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "The distance-function factory is missing from the resource at " + path);
        }

        // Quantization params for lazy quantizer creation at query time
        VTreeQuantizationParams quantizationParams = null;
        if (hasQuantizationParams()) {
            quantizationParams =
                    new VTreeQuantizationParams(minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount);
        }

        return LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, fileRef,
                storageManager.getBufferCache(ncServiceCtx), typeTraits, cmpFactories, BLOOM_FILTER_FALSE_POSITIVE_RATE,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, pageWriteCallbackFactory,
                vectorDimensions, vectorFields, filterFields, null, // filterFrameFactory
                null, // filterManager
                null, // filterHelper
                durable, metadataPageManagerFactory, atomic, null, vectorAccessorFactory, numPrimaryKeyFields,
                numIncludeFields, dataTupleBuilderFactory, quantizationParams, distanceFunctionFactory,
                crossPollination);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        appendToJson(jsonObject, registry); // Call this.appendToJson() to include quantization params
        return jsonObject;
    }

    @Override
    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        super.appendToJson(json, registry);
        json.put("vectorDimensions", vectorDimensions);
        json.putPOJO("vectorFields", vectorFields);
        json.putPOJO("filterFields", filterFields);
        json.put("atomic", atomic);
        // Cross-pollination placement params — only when active (M>1); absence reads back as legacy M=1.
        if (crossPollination != null && crossPollination.enabled()) {
            json.put(KEY_CROSS_POLLINATION_M, crossPollination.m());
            json.put(KEY_RNG_FACTOR, crossPollination.rngFactor());
            json.put(KEY_EPSILON, crossPollination.epsilon());
        }
        // Vector accessor factory — round-tripped via IPersistedResourceRegistry. The factory
        // implementation (AOrderedListVectorBinaryAccessorFactory) must be registered with the
        // registry; see PersistedResourceRegistry#registerClasses.
        if (vectorAccessorFactory != null) {
            json.set(KEY_VECTOR_ACCESSOR_FACTORY, vectorAccessorFactory.toJson(registry));
        }
        // Distance-function factory — round-tripped via the registry (implementation must be
        // registered; see PersistedResourceRegistry#registerClasses).
        if (distanceFunctionFactory != null) {
            json.set(KEY_DISTANCE_FUNCTION_FACTORY, distanceFunctionFactory.toJson(registry));
        }
        // Write quantization parameters only when set (a non-quantized index has none).
        putIfNotNull(json, KEY_CONFIDENCE_INTERVAL, confidenceInterval);
        putIfNotNull(json, KEY_MIN_QUANTILE, minQuantile);
        putIfNotNull(json, KEY_MAX_QUANTILE, maxQuantile);
        putIfNotNull(json, KEY_ALPHA, alpha);
        putIfNotNull(json, KEY_BITS, bits);
        putIfNotNull(json, KEY_SAMPLE_COUNT, sampleCount);
        json.put("numPrimaryKeyFields", numPrimaryKeyFields);
        json.put("numIncludeFields", numIncludeFields);
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        // appendToJson always writes vectorDimensions, so its absence (or a non-positive value) means the
        // resource is corrupt/foreign. Fail fast with a clear message instead of carrying -1 into deeper,
        // less obvious failures. (numPrimaryKeyFields/numIncludeFields keep plausible back-compat defaults.)
        if (!json.has("vectorDimensions") || json.get("vectorDimensions").asInt() <= 0) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "LSMVTreeLocalResource is missing or has a non-positive vectorDimensions; resource is corrupt");
        }
        int vectorDimensions = json.get("vectorDimensions").asInt();
        int numPrimaryKeyFields = json.has("numPrimaryKeyFields") ? json.get("numPrimaryKeyFields").asInt() : 1;
        int numIncludeFields = json.has("numIncludeFields") ? json.get("numIncludeFields").asInt() : 0;
        int[] vectorFields =
                json.has("vectorFields") ? OBJECT_MAPPER.convertValue(json.get("vectorFields"), int[].class) : null;
        int[] filterFields =
                json.has("filterFields") ? OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class) : null;
        boolean atomic = json.has("atomic") && json.get("atomic").asBoolean();

        // Vector accessor factory — required on read; createInstance() throws if absent. New
        // resources always carry it (see appendToJson). A missing key indicates a corrupted or
        // unsupported-version JSON file.
        IVTreeBinaryAccessorFactory vectorAccessorFactory = json.has(KEY_VECTOR_ACCESSOR_FACTORY)
                ? (IVTreeBinaryAccessorFactory) registry.deserialize(json.get(KEY_VECTOR_ACCESSOR_FACTORY)) : null;

        // Distance-function factory — required on read; createInstance() throws if absent. New resources
        // always carry it. Missing key => corrupted or unsupported-version JSON.
        IVTreeDistanceFunctionFactory distanceFunctionFactory = json.has(KEY_DISTANCE_FUNCTION_FACTORY)
                ? (IVTreeDistanceFunctionFactory) registry.deserialize(json.get(KEY_DISTANCE_FUNCTION_FACTORY)) : null;

        // Read quantization parameters with backward compatibility (missing → null).
        Float confidenceInterval = readOptionalFloat(json, KEY_CONFIDENCE_INTERVAL);
        Float minQuantile = readOptionalFloat(json, KEY_MIN_QUANTILE);
        Float maxQuantile = readOptionalFloat(json, KEY_MAX_QUANTILE);
        Float alpha = readOptionalFloat(json, KEY_ALPHA);
        Integer bits = readOptionalInt(json, KEY_BITS);
        Integer sampleCount = readOptionalInt(json, KEY_SAMPLE_COUNT);
        // Determine quantized vs non-quantized based on presence of quantization parameters
        boolean isQuantized = (minQuantile != null);
        IVTreeDataTupleBuilderFactory dataTupleBuilderFactory =
                new VTreeDataTupleBuilderFactory(numIncludeFields, isQuantized);

        // Cross-pollination params — back-compat: absent (or M<=1) reads back as legacy single-closest.
        CrossPollinationConfig crossPollination = CrossPollinationConfig.LEGACY;
        if (json.has(KEY_CROSS_POLLINATION_M)) {
            int m = json.get(KEY_CROSS_POLLINATION_M).asInt(1);
            double rngFactor = json.has(KEY_RNG_FACTOR) ? json.get(KEY_RNG_FACTOR).asDouble(1.0) : 1.0;
            double epsilon = json.has(KEY_EPSILON) ? json.get(KEY_EPSILON).asDouble(0.3) : 0.3;
            crossPollination = new CrossPollinationConfig(m, rngFactor, epsilon);
        }

        return new LSMVTreeLocalResource(registry, json, vectorDimensions, vectorFields, filterFields, atomic,
                confidenceInterval, minQuantile, maxQuantile, alpha, bits, sampleCount, vectorAccessorFactory,
                numPrimaryKeyFields, numIncludeFields, dataTupleBuilderFactory, distanceFunctionFactory,
                crossPollination);
    }

    /** Read an optional float field from JSON; returns {@code null} if the field is missing or null. */
    private static Float readOptionalFloat(JsonNode json, String fieldName) {
        if (!json.has(fieldName)) {
            return null;
        }
        JsonNode node = json.get(fieldName);
        return node.isNull() ? null : (float) node.asDouble();
    }

    /** Read an optional int field from JSON; returns {@code null} if the field is missing or null. */
    private static Integer readOptionalInt(JsonNode json, String fieldName) {
        if (!json.has(fieldName)) {
            return null;
        }
        JsonNode node = json.get(fieldName);
        return node.isNull() ? null : node.asInt();
    }

    private static void putIfNotNull(ObjectNode json, String fieldName, Float value) {
        if (value != null) {
            json.put(fieldName, value);
        }
    }

    private static void putIfNotNull(ObjectNode json, String fieldName, Integer value) {
        if (value != null) {
            json.put(fieldName, value);
        }
    }

    public Float getConfidenceInterval() {
        return confidenceInterval;
    }

    public Float getMinQuantile() {
        return minQuantile;
    }

    public Float getMaxQuantile() {
        return maxQuantile;
    }

    public Float getAlpha() {
        return alpha;
    }

    public Integer getBits() {
        return bits;
    }

    public Integer getSampleCount() {
        return sampleCount;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public int[] getVectorFields() {
        return vectorFields;
    }

    public int[] getFilterFields() {
        return filterFields;
    }

    public boolean isAtomic() {
        return atomic;
    }

    /** @return true iff all required quantization parameters are present. */
    public boolean hasQuantizationParams() {
        return bits != null && confidenceInterval != null && minQuantile != null && maxQuantile != null
                && alpha != null;
    }

    @Override
    public void setQuantizationParameters(VTreeQuantizationParams parameters) {
        if (parameters == null) {
            return;
        }
        this.minQuantile = parameters.minQuantile();
        this.maxQuantile = parameters.maxQuantile();
        this.alpha = parameters.alpha();
        this.bits = parameters.bits();
        this.confidenceInterval = parameters.confidenceInterval();
        this.sampleCount = parameters.sampleCount();
    }
}
