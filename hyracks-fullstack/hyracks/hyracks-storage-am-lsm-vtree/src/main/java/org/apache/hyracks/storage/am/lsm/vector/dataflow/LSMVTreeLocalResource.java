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
import java.util.Objects;

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

    private static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    private static final String KEY_VECTOR_DIMENSIONS = "vectorDimensions";
    private static final String KEY_VECTOR_FIELDS = "vectorFields";
    private static final String KEY_ATOMIC = "atomic";
    private static final String KEY_IDENTITY_FIELDS = "identityFields";
    private static final String KEY_NUM_INCLUDE_FIELDS = "numIncludeFields";
    private static final String KEY_VECTOR_ACCESSOR_FACTORY = "vectorAccessorFactory";
    private static final String KEY_DISTANCE_FUNCTION_FACTORY = "distanceFunctionFactory";
    private static final String KEY_CROSS_POLLINATION_M = "crossPollinationM";
    private static final String KEY_RNG_FACTOR = "rngFactor";
    private static final String KEY_EPSILON = "epsilon";
    private static final String KEY_QUANTIZATION = "quantization";
    private static final String KEY_MIN_QUANTILE = "minQuantile";
    private static final String KEY_MAX_QUANTILE = "maxQuantile";
    private static final String KEY_ALPHA = "alpha";
    private static final String KEY_CONFIDENCE_INTERVAL = "confidenceInterval";
    private static final String KEY_BITS = "bits";
    private static final String KEY_SAMPLE_COUNT = "sampleCount";

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final boolean atomic;
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    /** Positions of the identity fields within a data tuple; the ordering key is distance plus these. */
    protected final int[] identityFields;
    /** How many INCLUDE fields the <em>input</em> tuple carries; an input-layout fact for the tuple builder. */
    protected final int numIncludeFields;

    /**
     * Distance-function factory supplied at DDL time. Persisted so a restarted index reconstructs the
     * same distance implementation.
     */
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;

    /**
     * Cross-pollination placement config supplied at DDL time; never {@code null}. Persisted so that
     * incremental insert and delete on a restarted index resolve the leaf clusters bulk-load used.
     */
    protected final CrossPollinationConfig crossPollination;

    /** Level-wise candidate window from the DDL; persisted for the same reason as {@link #crossPollination}. */
    protected final double epsilon;

    /**
     * Scalar-quantization calibration, or {@code null} for a non-quantized index. Written once by
     * {@link QuantizedIndexBuilder} between resource creation and the first {@link #createInstance}, so
     * its presence is the single answer to whether this index is quantized.
     */
    protected VTreeQuantizationParams quantization;

    public LSMVTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int[] identityFields, int numIncludeFields,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination,
            double epsilon) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, nullTypeTraits,
                nullIntrospector);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.atomic = atomic;
        this.vectorAccessorFactory = Objects.requireNonNull(vectorAccessorFactory, "vectorAccessorFactory");
        this.identityFields = Objects.requireNonNull(identityFields, "identityFields");
        this.numIncludeFields = numIncludeFields;
        this.distanceFunctionFactory = Objects.requireNonNull(distanceFunctionFactory, "distanceFunctionFactory");
        this.crossPollination = Objects.requireNonNull(crossPollination, "crossPollination");
        this.epsilon = epsilon;
    }

    protected LSMVTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, boolean atomic, IVTreeBinaryAccessorFactory vectorAccessorFactory, int[] identityFields,
            int numIncludeFields, IVTreeDistanceFunctionFactory distanceFunctionFactory,
            CrossPollinationConfig crossPollination, double epsilon, VTreeQuantizationParams quantization)
            throws HyracksDataException {
        super(registry, json);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.atomic = atomic;
        this.vectorAccessorFactory = Objects.requireNonNull(vectorAccessorFactory, "vectorAccessorFactory");
        this.identityFields = Objects.requireNonNull(identityFields, "identityFields");
        this.numIncludeFields = numIncludeFields;
        this.distanceFunctionFactory = Objects.requireNonNull(distanceFunctionFactory, "distanceFunctionFactory");
        this.crossPollination = Objects.requireNonNull(crossPollination, "crossPollination");
        this.epsilon = epsilon;
        this.quantization = quantization;
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        IIOManager ioManager = storageManager.getIoManager(ncServiceCtx);
        NCConfig storageConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
        FileReference fileRef = ioManager.resolve(path);

        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(ncServiceCtx, fileRef);
        ioOpCallbackFactory.initialize(ncServiceCtx, this);
        pageWriteCallbackFactory.initialize(ncServiceCtx, this);

        // The tuple layout follows from the persisted facts, so it is rebuilt here rather than carried as a
        // second copy of them.
        VTreeDataTupleBuilderFactory dataTupleBuilderFactory =
                new VTreeDataTupleBuilderFactory(numIncludeFields, identityFields.length, quantization != null);

        // A VTree has no LSM component filter; the base carries the filter traits only because every
        // LsmResource does.
        return LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, fileRef,
                storageManager.getBufferCache(ncServiceCtx), typeTraits, cmpFactories, BLOOM_FILTER_FALSE_POSITIVE_RATE,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, pageWriteCallbackFactory,
                vectorDimensions, vectorFields, filterFields, null, null, null, durable, metadataPageManagerFactory,
                atomic, null, vectorAccessorFactory, identityFields, dataTupleBuilderFactory, quantization,
                distanceFunctionFactory, crossPollination, epsilon);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        appendToJson(jsonObject, registry);
        return jsonObject;
    }

    @Override
    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        super.appendToJson(json, registry);
        json.put(KEY_VECTOR_DIMENSIONS, vectorDimensions);
        json.putPOJO(KEY_VECTOR_FIELDS, vectorFields);
        json.put(KEY_ATOMIC, atomic);
        json.putPOJO(KEY_IDENTITY_FIELDS, identityFields);
        json.put(KEY_NUM_INCLUDE_FIELDS, numIncludeFields);
        // The placement parameters are written at every M. A restarted NC that reconstructed the index with
        // a different window than bulk-load used would resolve a delete to another leaf cluster and leak it.
        json.put(KEY_EPSILON, epsilon);
        json.put(KEY_CROSS_POLLINATION_M, crossPollination.m());
        json.put(KEY_RNG_FACTOR, crossPollination.rngFactor());
        // Both factories round-trip through the registry, so their implementations must be registered there;
        // see PersistedResourceRegistry#registerClasses.
        json.set(KEY_VECTOR_ACCESSOR_FACTORY, vectorAccessorFactory.toJson(registry));
        json.set(KEY_DISTANCE_FUNCTION_FACTORY, distanceFunctionFactory.toJson(registry));
        if (quantization != null) {
            ObjectNode quantizationNode = OBJECT_MAPPER.createObjectNode();
            quantizationNode.put(KEY_MIN_QUANTILE, quantization.minQuantile());
            quantizationNode.put(KEY_MAX_QUANTILE, quantization.maxQuantile());
            quantizationNode.put(KEY_ALPHA, quantization.alpha());
            quantizationNode.put(KEY_CONFIDENCE_INTERVAL, quantization.confidenceInterval());
            quantizationNode.put(KEY_BITS, quantization.bits());
            quantizationNode.put(KEY_SAMPLE_COUNT, quantization.sampleCount());
            json.set(KEY_QUANTIZATION, quantizationNode);
        }
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        int vectorDimensions = require(json, KEY_VECTOR_DIMENSIONS).asInt();
        if (vectorDimensions <= 0) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "LSMVTreeLocalResource carries a non-positive " + KEY_VECTOR_DIMENSIONS + "; resource is corrupt");
        }
        int[] identityFields = OBJECT_MAPPER.convertValue(require(json, KEY_IDENTITY_FIELDS), int[].class);
        int numIncludeFields = require(json, KEY_NUM_INCLUDE_FIELDS).asInt();
        int[] vectorFields = OBJECT_MAPPER.convertValue(require(json, KEY_VECTOR_FIELDS), int[].class);
        boolean atomic = json.has(KEY_ATOMIC) && json.get(KEY_ATOMIC).asBoolean();
        IVTreeBinaryAccessorFactory vectorAccessorFactory =
                (IVTreeBinaryAccessorFactory) registry.deserialize(require(json, KEY_VECTOR_ACCESSOR_FACTORY));
        IVTreeDistanceFunctionFactory distanceFunctionFactory =
                (IVTreeDistanceFunctionFactory) registry.deserialize(require(json, KEY_DISTANCE_FUNCTION_FACTORY));
        double epsilon = require(json, KEY_EPSILON).asDouble();
        CrossPollinationConfig crossPollination = new CrossPollinationConfig(
                require(json, KEY_CROSS_POLLINATION_M).asInt(), require(json, KEY_RNG_FACTOR).asDouble());

        VTreeQuantizationParams quantization = null;
        if (json.has(KEY_QUANTIZATION)) {
            JsonNode node = json.get(KEY_QUANTIZATION);
            quantization = new VTreeQuantizationParams((float) require(node, KEY_MIN_QUANTILE).asDouble(),
                    (float) require(node, KEY_MAX_QUANTILE).asDouble(), (float) require(node, KEY_ALPHA).asDouble(),
                    (float) require(node, KEY_CONFIDENCE_INTERVAL).asDouble(), require(node, KEY_BITS).asInt(),
                    require(node, KEY_SAMPLE_COUNT).asInt());
        }
        return new LSMVTreeLocalResource(registry, json, vectorDimensions, vectorFields, atomic, vectorAccessorFactory,
                identityFields, numIncludeFields, distanceFunctionFactory, crossPollination, epsilon, quantization);
    }

    /**
     * Read a key {@link #appendToJson} always writes. Its absence means the file was written by another
     * class or truncated, and substituting a default would silently mis-key the index.
     *
     * @throws HyracksDataException if the key is missing or null.
     */
    private static JsonNode require(JsonNode json, String key) throws HyracksDataException {
        JsonNode node = json.get(key);
        if (node == null || node.isNull()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "LSMVTreeLocalResource is missing `" + key + "`; resource is corrupt");
        }
        return node;
    }

    /** @return the scalar-quantization calibration, or {@code null} for a non-quantized index. */
    public VTreeQuantizationParams getQuantizationParams() {
        return quantization;
    }

    @Override
    public void setQuantizationParameters(VTreeQuantizationParams parameters) {
        if (parameters == null) {
            return;
        }
        this.quantization = parameters;
    }
}
