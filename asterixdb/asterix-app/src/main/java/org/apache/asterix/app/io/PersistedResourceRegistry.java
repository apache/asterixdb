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
package org.apache.asterix.app.io;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.DatasetInfoProvider;
import org.apache.asterix.common.context.DatasetLSMComponentIdGeneratorFactory;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.ioopcallbacks.LSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.dataflow.data.common.AListElementTokenFactory;
import org.apache.asterix.dataflow.data.common.AOrderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.common.AUnorderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectDescBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.BooleanBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.LongBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.RawBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.PrimitiveValueProviderFactory;
import org.apache.asterix.formats.nontagged.AnyBinaryComparatorFactory;
import org.apache.asterix.formats.nontagged.OrderedBinaryComparatorFactory;
import org.apache.asterix.formats.nontagged.OrderedLinearizeComparatorFactory;
import org.apache.asterix.metadata.utils.SecondaryCorrelatedTreeIndexOperationsHelper;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.RawUTF8StringPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercasePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercaseTokenPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.storage.am.common.data.PointablePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyLocalResource;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexLocalResource;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.HashedUTF8NGramTokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.HashedUTF8WordTokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterLocalResource;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.DoublePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.FloatPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.IntegerPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;
import org.apache.hyracks.storage.common.LocalResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PersistedResourceRegistry implements IPersistedResourceRegistry {

    private static final String DESERIALIZATION_METHOD = "fromJson";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    protected static final Map<String, Class<? extends IJsonSerializable>> REGISTERED_CLASSES = new HashMap<>();

    public PersistedResourceRegistry() {
        registerClasses();
        ensureFromJsonMethod();
    }

    protected void registerClasses() {
        /* WARNING: Changing a resource id will break storage format backward compatibility.*/
        REGISTERED_CLASSES.put("Checkpoint", Checkpoint.class);

        // IResource
        REGISTERED_CLASSES.put("LocalResource", LocalResource.class);
        REGISTERED_CLASSES.put("DatasetLocalResource", DatasetLocalResource.class);
        REGISTERED_CLASSES.put("LSMBTreeLocalResource", LSMBTreeLocalResource.class);
        REGISTERED_CLASSES.put("LSMRTreeLocalResource", LSMRTreeLocalResource.class);
        REGISTERED_CLASSES.put("LSMRTreeWithAntiMatterLocalResource", LSMRTreeWithAntiMatterLocalResource.class);
        REGISTERED_CLASSES.put("LSMInvertedIndexLocalResource", LSMInvertedIndexLocalResource.class);
        REGISTERED_CLASSES.put("ExternalBTreeLocalResource", ExternalBTreeLocalResource.class);
        REGISTERED_CLASSES.put("ExternalBTreeWithBuddyLocalResource", ExternalBTreeWithBuddyLocalResource.class);
        REGISTERED_CLASSES.put("ExternalRTreeLocalResource", ExternalRTreeLocalResource.class);

        // ILSMMergePolicyFactory
        REGISTERED_CLASSES.put("NoMergePolicyFactory", NoMergePolicyFactory.class);
        REGISTERED_CLASSES.put("PrefixMergePolicyFactory", PrefixMergePolicyFactory.class);
        REGISTERED_CLASSES.put("ConstantMergePolicyFactory", ConstantMergePolicyFactory.class);
        REGISTERED_CLASSES.put("CorrelatedPrefixMergePolicyFactory", CorrelatedPrefixMergePolicyFactory.class);

        // ILSMIOOperationSchedulerProvider
        REGISTERED_CLASSES.put("RuntimeComponentsProvider", RuntimeComponentsProvider.class);

        // ITypeTraits
        REGISTERED_CLASSES.put("FixedLengthTypeTrait", FixedLengthTypeTrait.class);
        REGISTERED_CLASSES.put("VarLengthTypeTrait", VarLengthTypeTrait.class);

        // ILSMOperationTrackerFactory
        REGISTERED_CLASSES.put("PrimaryIndexOperationTrackerFactory", PrimaryIndexOperationTrackerFactory.class);
        REGISTERED_CLASSES.put("SecondaryIndexOperationTrackerFactory", SecondaryIndexOperationTrackerFactory.class);

        // ILSMComponentIdGeneratorFactory
        REGISTERED_CLASSES.put("DatasetLSMComponentIdGeneratorFactory", DatasetLSMComponentIdGeneratorFactory.class);

        // IDatasetInfoProvider
        REGISTERED_CLASSES.put("DatasetInfoProvider", DatasetInfoProvider.class);

        // ILSMOperationTrackerFactory
        REGISTERED_CLASSES.put("NoOpIOOperationCallbackFactory", NoOpIOOperationCallbackFactory.class);
        REGISTERED_CLASSES.put("LSMBTreeIOOperationCallbackFactory", LSMIndexIOOperationCallbackFactory.class);

        // ILSMIOOperationSchedulerProvider
        REGISTERED_CLASSES.put("AppendOnlyLinkedMetadataPageManagerFactory",
                AppendOnlyLinkedMetadataPageManagerFactory.class);

        // ILSMIOOperationSchedulerProvider
        REGISTERED_CLASSES.put("AsterixVirtualBufferCacheProvider", AsterixVirtualBufferCacheProvider.class);

        // IBinaryComparatorFactory
        REGISTERED_CLASSES.put("ACirclePartialBinaryComparatorFactory", ACirclePartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("ADurationPartialBinaryComparatorFactory",
                ADurationPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("AIntervalAscPartialBinaryComparatorFactory",
                AIntervalAscPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("AIntervalDescPartialBinaryComparatorFactory",
                AIntervalDescPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("ALinePartialBinaryComparatorFactory", ALinePartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("AObjectAscBinaryComparatorFactory", AObjectAscBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("AObjectDescBinaryComparatorFactory", AObjectDescBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("APoint3DPartialBinaryComparatorFactory", APoint3DPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("APointPartialBinaryComparatorFactory", APointPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("APolygonPartialBinaryComparatorFactory", APolygonPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("ARectanglePartialBinaryComparatorFactory",
                ARectanglePartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("AUUIDPartialBinaryComparatorFactory", AUUIDPartialBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("BooleanBinaryComparatorFactory", BooleanBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("ListItemBinaryComparatorFactory", ListItemBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("LongBinaryComparatorFactory", LongBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("RawBinaryComparatorFactory", RawBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("PointableBinaryComparatorFactory", PointableBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("HilbertDoubleComparatorFactory", HilbertDoubleComparatorFactory.class);
        REGISTERED_CLASSES.put("ZCurveDoubleComparatorFactory", ZCurveDoubleComparatorFactory.class);
        REGISTERED_CLASSES.put("ZCurveIntComparatorFactory", ZCurveIntComparatorFactory.class);
        REGISTERED_CLASSES.put("ComponentPosComparatorFactory",
                SecondaryCorrelatedTreeIndexOperationsHelper.ComponentPosComparatorFactory.class);
        REGISTERED_CLASSES.put("AnyBinaryComparatorFactory", AnyBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("OrderedBinaryComparatorFactory", OrderedBinaryComparatorFactory.class);
        REGISTERED_CLASSES.put("OrderedLinearizeComparatorFactory", OrderedLinearizeComparatorFactory.class);

        // IPointableFactory
        REGISTERED_CLASSES.put("AIntervalPointableFactory", AIntervalPointable.AIntervalPointableFactory.class);
        REGISTERED_CLASSES.put("AListPointableFactory", AListPointable.AListPointableFactory.class);
        REGISTERED_CLASSES.put("ARecordPointableFactory", ARecordPointable.ARecordPointableFactory.class);
        REGISTERED_CLASSES.put("BooleanPointableFactory", BooleanPointable.BooleanPointableFactory.class);
        REGISTERED_CLASSES.put("ByteArrayPointableFactory", ByteArrayPointable.ByteArrayPointableFactory.class);
        REGISTERED_CLASSES.put("BytePointableFactory", BytePointable.BytePointableFactory.class);
        REGISTERED_CLASSES.put("DoublePointableFactory", DoublePointable.DoublePointableFactory.class);
        REGISTERED_CLASSES.put("FloatPointableFactory", FloatPointable.FloatPointableFactory.class);
        REGISTERED_CLASSES.put("IntegerPointableFactory", IntegerPointable.IntegerPointableFactory.class);
        REGISTERED_CLASSES.put("LongPointableFactory", LongPointable.LongPointableFactory.class);
        REGISTERED_CLASSES.put("RawUTF8StringPointableFactory",
                RawUTF8StringPointable.RawUTF8StringPointableFactory.class);
        REGISTERED_CLASSES.put("ShortPointableFactory", ShortPointable.ShortPointableFactory.class);
        REGISTERED_CLASSES.put("TaggedValuePointableFactory", TaggedValuePointable.TaggedValuePointableFactory.class);
        REGISTERED_CLASSES.put("UTF8StringLowercasePointableFactory",
                UTF8StringLowercasePointable.UTF8StringLowercasePointableFactory.class);
        REGISTERED_CLASSES.put("UTF8StringLowercaseTokenPointableFactory",
                UTF8StringLowercaseTokenPointable.UTF8StringLowercaseTokenPointableFactory.class);
        REGISTERED_CLASSES.put("UTF8StringPointableFactory", UTF8StringPointable.UTF8StringPointableFactory.class);
        REGISTERED_CLASSES.put("VoidPointableFactory", VoidPointable.VoidPointableFactory.class);

        // IPrimitiveValueProviderFactory
        REGISTERED_CLASSES.put("DoublePrimitiveValueProviderFactory", DoublePrimitiveValueProviderFactory.class);
        REGISTERED_CLASSES.put("FloatPrimitiveValueProviderFactory", FloatPrimitiveValueProviderFactory.class);
        REGISTERED_CLASSES.put("IntegerPrimitiveValueProviderFactory", IntegerPrimitiveValueProviderFactory.class);
        REGISTERED_CLASSES.put("PointablePrimitiveValueProviderFactory", PointablePrimitiveValueProviderFactory.class);
        REGISTERED_CLASSES.put("PrimitiveValueProviderFactory", PrimitiveValueProviderFactory.class);

        // IBinaryTokenizerFactory
        REGISTERED_CLASSES.put("AOrderedListBinaryTokenizerFactory", AOrderedListBinaryTokenizerFactory.class);
        REGISTERED_CLASSES.put("AUnorderedListBinaryTokenizerFactory", AUnorderedListBinaryTokenizerFactory.class);
        REGISTERED_CLASSES.put("NGramUTF8StringBinaryTokenizerFactory", NGramUTF8StringBinaryTokenizerFactory.class);
        REGISTERED_CLASSES.put("DelimitedUTF8StringBinaryTokenizerFactory",
                DelimitedUTF8StringBinaryTokenizerFactory.class);

        // ITokenFactory
        REGISTERED_CLASSES.put("AListElementTokenFactory", AListElementTokenFactory.class);
        REGISTERED_CLASSES.put("HashedUTF8NGramTokenFactory", HashedUTF8NGramTokenFactory.class);
        REGISTERED_CLASSES.put("HashedUTF8WordTokenFactory", HashedUTF8WordTokenFactory.class);
        REGISTERED_CLASSES.put("UTF8NGramTokenFactory", UTF8NGramTokenFactory.class);
        REGISTERED_CLASSES.put("UTF8WordTokenFactory", UTF8WordTokenFactory.class);
        REGISTERED_CLASSES.put("RTreePolicyType", RTreePolicyType.class);
    }

    @Override
    public IJsonSerializable deserialize(JsonNode json) throws HyracksDataException {
        try {
            String resourceId = json.get(TYPE_FIELD_ID).asText();
            Class<? extends IJsonSerializable> clazz = getResourceClass(resourceId);

            //Using static method (fromJson)
            Method method = clazz.getMethod(DESERIALIZATION_METHOD, IPersistedResourceRegistry.class, JsonNode.class);
            return (IJsonSerializable) method.invoke(null, this, json);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public ObjectNode getClassIdentifier(Class<? extends IJsonSerializable> clazz, long version) {
        final ObjectNode objectNode = JSON_MAPPER.createObjectNode();
        objectNode.put(IPersistedResourceRegistry.TYPE_FIELD_ID, getResourceId(clazz));
        objectNode.put(IPersistedResourceRegistry.VERSION_FIELD_ID, version);
        objectNode.put(IPersistedResourceRegistry.CLASS_FIELD_ID, clazz.getName());
        return objectNode;
    }

    private String getResourceId(Class<? extends IJsonSerializable> clazz) {
        Optional<String> classId = REGISTERED_CLASSES.entrySet().stream()
                .filter(entry -> Objects.equals(entry.getValue(), clazz)).map(Map.Entry::getKey).findAny();
        if (classId.isPresent()) {
            return classId.get();
        }
        throw new IllegalStateException(String.format("Class %s was not registered.", clazz.getName()));
    }

    private Class<? extends IJsonSerializable> getResourceClass(String id) {
        return REGISTERED_CLASSES.computeIfAbsent(id, key -> {
            throw new IllegalStateException(String.format("No class with id %s was registered.", key));
        });
    }

    protected static void ensureFromJsonMethod() {
        for (Class<?> clazz : REGISTERED_CLASSES.values()) {
            try {
                // Ensure fromJson method exists with expected parameters
                clazz.getMethod(DESERIALIZATION_METHOD, IPersistedResourceRegistry.class, JsonNode.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(String
                        .format("Registered class %s must provide a static method fromJson(IPersistedResourceRegistry,"
                                + " JsonNode)", clazz.getName()),
                        e);
            }
        }
    }
}
