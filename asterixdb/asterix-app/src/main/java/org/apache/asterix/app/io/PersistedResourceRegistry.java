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
import org.apache.asterix.common.ioopcallbacks.LSMIndexPageWriteCallbackFactory;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.dataflow.data.common.AListElementTokenFactory;
import org.apache.asterix.dataflow.data.common.AOrderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.common.AUnorderedListBinaryTokenizerFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AGenericAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AGenericDescBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.PrimitiveValueProviderFactory;
import org.apache.asterix.formats.nontagged.AnyBinaryComparatorFactory;
import org.apache.asterix.formats.nontagged.OrderedBinaryComparatorFactory;
import org.apache.asterix.formats.nontagged.OrderedLinearizeComparatorFactory;
import org.apache.asterix.metadata.utils.SecondaryCorrelatedTreeIndexOperationsHelper;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.accessors.BooleanBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.ByteArrayBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.ByteBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.FloatBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.ShortBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringLowercaseBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringLowercaseTokenBinaryComparatorFactory;
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
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicyFactory;
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
    protected final Map<String, Class<? extends IJsonSerializable>> registeredClasses = new HashMap<>();

    public PersistedResourceRegistry() {
        registerClasses();
        ensureFromJsonMethod();
    }

    protected void registerClasses() {
        /* WARNING: Changing a resource id will break storage format backward compatibility.*/
        registeredClasses.put("Checkpoint", Checkpoint.class);

        // IResource
        registeredClasses.put("LocalResource", LocalResource.class);
        registeredClasses.put("DatasetLocalResource", DatasetLocalResource.class);
        registeredClasses.put("LSMBTreeLocalResource", LSMBTreeLocalResource.class);
        registeredClasses.put("LSMRTreeLocalResource", LSMRTreeLocalResource.class);
        registeredClasses.put("LSMRTreeWithAntiMatterLocalResource", LSMRTreeWithAntiMatterLocalResource.class);
        registeredClasses.put("LSMInvertedIndexLocalResource", LSMInvertedIndexLocalResource.class);
        registeredClasses.put("ExternalBTreeLocalResource", ExternalBTreeLocalResource.class);
        registeredClasses.put("ExternalBTreeWithBuddyLocalResource", ExternalBTreeWithBuddyLocalResource.class);
        registeredClasses.put("ExternalRTreeLocalResource", ExternalRTreeLocalResource.class);

        // ILSMMergePolicyFactory
        registeredClasses.put("NoMergePolicyFactory", NoMergePolicyFactory.class);
        registeredClasses.put("PrefixMergePolicyFactory", PrefixMergePolicyFactory.class);
        registeredClasses.put("ConcurrentMergePolicyFactory", ConcurrentMergePolicyFactory.class);
        registeredClasses.put("ConstantMergePolicyFactory", ConstantMergePolicyFactory.class);
        registeredClasses.put("CorrelatedPrefixMergePolicyFactory", CorrelatedPrefixMergePolicyFactory.class);

        // ILSMIOOperationSchedulerProvider
        registeredClasses.put("RuntimeComponentsProvider", RuntimeComponentsProvider.class);

        // ITypeTraits
        registeredClasses.put("FixedLengthTypeTrait", FixedLengthTypeTrait.class);
        registeredClasses.put("VarLengthTypeTrait", VarLengthTypeTrait.class);

        // ILSMOperationTrackerFactory
        registeredClasses.put("PrimaryIndexOperationTrackerFactory", PrimaryIndexOperationTrackerFactory.class);
        registeredClasses.put("SecondaryIndexOperationTrackerFactory", SecondaryIndexOperationTrackerFactory.class);

        // ILSMComponentIdGeneratorFactory
        registeredClasses.put("DatasetLSMComponentIdGeneratorFactory", DatasetLSMComponentIdGeneratorFactory.class);

        // IDatasetInfoProvider
        registeredClasses.put("DatasetInfoProvider", DatasetInfoProvider.class);

        // ILSMOperationTrackerFactory
        registeredClasses.put("NoOpIOOperationCallbackFactory", NoOpIOOperationCallbackFactory.class);
        registeredClasses.put("LSMBTreeIOOperationCallbackFactory", LSMIndexIOOperationCallbackFactory.class);
        registeredClasses.put("LSMIndexPageWriteCallbackFactory", LSMIndexPageWriteCallbackFactory.class);

        // ILSMIOOperationSchedulerProvider
        registeredClasses.put("AppendOnlyLinkedMetadataPageManagerFactory",
                AppendOnlyLinkedMetadataPageManagerFactory.class);

        // ILSMIOOperationSchedulerProvider
        registeredClasses.put("AsterixVirtualBufferCacheProvider", AsterixVirtualBufferCacheProvider.class);

        // IBinaryComparatorFactory
        registeredClasses.put("ACirclePartialBinaryComparatorFactory", ACirclePartialBinaryComparatorFactory.class);
        registeredClasses.put("ADurationPartialBinaryComparatorFactory", ADurationPartialBinaryComparatorFactory.class);
        registeredClasses.put("AIntervalAscPartialBinaryComparatorFactory",
                AIntervalAscPartialBinaryComparatorFactory.class);
        registeredClasses.put("AIntervalDescPartialBinaryComparatorFactory",
                AIntervalDescPartialBinaryComparatorFactory.class);
        registeredClasses.put("ALinePartialBinaryComparatorFactory", ALinePartialBinaryComparatorFactory.class);
        registeredClasses.put("AObjectAscBinaryComparatorFactory", AGenericAscBinaryComparatorFactory.class);
        registeredClasses.put("AObjectDescBinaryComparatorFactory", AGenericDescBinaryComparatorFactory.class);
        registeredClasses.put("APoint3DPartialBinaryComparatorFactory", APoint3DPartialBinaryComparatorFactory.class);
        registeredClasses.put("APointPartialBinaryComparatorFactory", APointPartialBinaryComparatorFactory.class);
        registeredClasses.put("APolygonPartialBinaryComparatorFactory", APolygonPartialBinaryComparatorFactory.class);
        registeredClasses.put("ARectanglePartialBinaryComparatorFactory",
                ARectanglePartialBinaryComparatorFactory.class);
        registeredClasses.put("AUUIDPartialBinaryComparatorFactory", AUUIDPartialBinaryComparatorFactory.class);
        registeredClasses.put("BooleanBinaryComparatorFactory", BooleanBinaryComparatorFactory.class);
        registeredClasses.put("ListItemBinaryComparatorFactory", ListItemBinaryComparatorFactory.class);
        registeredClasses.put("LongBinaryComparatorFactory", LongBinaryComparatorFactory.class);
        registeredClasses.put("RawBinaryComparatorFactory", RawBinaryComparatorFactory.class);
        registeredClasses.put("PointableBinaryComparatorFactory", PointableBinaryComparatorFactory.class);
        registeredClasses.put("HilbertDoubleComparatorFactory", HilbertDoubleComparatorFactory.class);
        registeredClasses.put("ZCurveDoubleComparatorFactory", ZCurveDoubleComparatorFactory.class);
        registeredClasses.put("ZCurveIntComparatorFactory", ZCurveIntComparatorFactory.class);
        registeredClasses.put("ComponentPosComparatorFactory",
                SecondaryCorrelatedTreeIndexOperationsHelper.ComponentPosComparatorFactory.class);
        registeredClasses.put("AnyBinaryComparatorFactory", AnyBinaryComparatorFactory.class);
        registeredClasses.put("OrderedBinaryComparatorFactory", OrderedBinaryComparatorFactory.class);
        registeredClasses.put("OrderedLinearizeComparatorFactory", OrderedLinearizeComparatorFactory.class);
        registeredClasses.put("ByteBinaryComparatorFactory", ByteBinaryComparatorFactory.class);
        registeredClasses.put("ShortBinaryComparatorFactory", ShortBinaryComparatorFactory.class);
        registeredClasses.put("IntegerBinaryComparatorFactory", IntegerBinaryComparatorFactory.class);
        registeredClasses.put("FloatBinaryComparatorFactory", FloatBinaryComparatorFactory.class);
        registeredClasses.put("DoubleBinaryComparatorFactory", DoubleBinaryComparatorFactory.class);
        registeredClasses.put("UTF8StringBinaryComparatorFactory", UTF8StringBinaryComparatorFactory.class);
        registeredClasses.put("UTF8StringLowercaseBinaryComparatorFactory",
                UTF8StringLowercaseBinaryComparatorFactory.class);
        registeredClasses.put("UTF8StringLowercaseTokenBinaryComparatorFactory",
                UTF8StringLowercaseTokenBinaryComparatorFactory.class);
        registeredClasses.put("ByteArrayBinaryComparatorFactory", ByteArrayBinaryComparatorFactory.class);

        // IPointableFactory
        registeredClasses.put("AIntervalPointableFactory", AIntervalPointable.AIntervalPointableFactory.class);
        registeredClasses.put("AListPointableFactory", AListPointable.AListPointableFactory.class);
        registeredClasses.put("ARecordPointableFactory", ARecordPointable.ARecordPointableFactory.class);
        registeredClasses.put("BooleanPointableFactory", BooleanPointable.BooleanPointableFactory.class);
        registeredClasses.put("ByteArrayPointableFactory", ByteArrayPointable.ByteArrayPointableFactory.class);
        registeredClasses.put("BytePointableFactory", BytePointable.BytePointableFactory.class);
        registeredClasses.put("DoublePointableFactory", DoublePointable.DoublePointableFactory.class);
        registeredClasses.put("FloatPointableFactory", FloatPointable.FloatPointableFactory.class);
        registeredClasses.put("IntegerPointableFactory", IntegerPointable.IntegerPointableFactory.class);
        registeredClasses.put("LongPointableFactory", LongPointable.LongPointableFactory.class);
        registeredClasses.put("RawUTF8StringPointableFactory",
                RawUTF8StringPointable.RawUTF8StringPointableFactory.class);
        registeredClasses.put("ShortPointableFactory", ShortPointable.ShortPointableFactory.class);
        registeredClasses.put("TaggedValuePointableFactory", TaggedValuePointable.TaggedValuePointableFactory.class);
        registeredClasses.put("UTF8StringLowercasePointableFactory",
                UTF8StringLowercasePointable.UTF8StringLowercasePointableFactory.class);
        registeredClasses.put("UTF8StringLowercaseTokenPointableFactory",
                UTF8StringLowercaseTokenPointable.UTF8StringLowercaseTokenPointableFactory.class);
        registeredClasses.put("UTF8StringPointableFactory", UTF8StringPointable.UTF8StringPointableFactory.class);
        registeredClasses.put("VoidPointableFactory", VoidPointable.VoidPointableFactory.class);

        // IPrimitiveValueProviderFactory
        registeredClasses.put("DoublePrimitiveValueProviderFactory", DoublePrimitiveValueProviderFactory.class);
        registeredClasses.put("FloatPrimitiveValueProviderFactory", FloatPrimitiveValueProviderFactory.class);
        registeredClasses.put("IntegerPrimitiveValueProviderFactory", IntegerPrimitiveValueProviderFactory.class);
        registeredClasses.put("PointablePrimitiveValueProviderFactory", PointablePrimitiveValueProviderFactory.class);
        registeredClasses.put("PrimitiveValueProviderFactory", PrimitiveValueProviderFactory.class);

        // IBinaryTokenizerFactory
        registeredClasses.put("AOrderedListBinaryTokenizerFactory", AOrderedListBinaryTokenizerFactory.class);
        registeredClasses.put("AUnorderedListBinaryTokenizerFactory", AUnorderedListBinaryTokenizerFactory.class);
        registeredClasses.put("NGramUTF8StringBinaryTokenizerFactory", NGramUTF8StringBinaryTokenizerFactory.class);
        registeredClasses.put("DelimitedUTF8StringBinaryTokenizerFactory",
                DelimitedUTF8StringBinaryTokenizerFactory.class);

        // ITokenFactory
        registeredClasses.put("AListElementTokenFactory", AListElementTokenFactory.class);
        registeredClasses.put("HashedUTF8NGramTokenFactory", HashedUTF8NGramTokenFactory.class);
        registeredClasses.put("HashedUTF8WordTokenFactory", HashedUTF8WordTokenFactory.class);
        registeredClasses.put("UTF8NGramTokenFactory", UTF8NGramTokenFactory.class);
        registeredClasses.put("UTF8WordTokenFactory", UTF8WordTokenFactory.class);
        registeredClasses.put("RTreePolicyType", RTreePolicyType.class);

        // IAType
        registeredClasses.put("BuiltinType", BuiltinType.class);
        registeredClasses.put("AOrderedListType", AOrderedListType.class);
        registeredClasses.put("ARecordType", ARecordType.class);
        registeredClasses.put("AUnionType", AUnionType.class);
        registeredClasses.put("AUnorderedListType", AUnorderedListType.class);

        //ICompressorDecompressorFactory
        CompressionManager.registerCompressorDecompressorsFactoryClasses(registeredClasses);
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
    public IJsonSerializable deserializeOrDefault(JsonNode json, Class<? extends IJsonSerializable> defaultClass)
            throws HyracksDataException {
        if (json != null) {
            return deserialize(json);
        }

        return deserializeDefault(defaultClass);
    }

    private IJsonSerializable deserializeDefault(Class<? extends IJsonSerializable> defaultClass)
            throws HyracksDataException {
        //Ensure it is registered
        final String resourceId = getResourceId(defaultClass);
        try {
            Class<? extends IJsonSerializable> clazz = getResourceClass(resourceId);
            //Using static method (fromJson)
            Method method = clazz.getMethod(DESERIALIZATION_METHOD, IPersistedResourceRegistry.class, JsonNode.class);
            return (IJsonSerializable) method.invoke(null, this, null);
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
        Optional<String> classId = registeredClasses.entrySet().stream()
                .filter(entry -> Objects.equals(entry.getValue(), clazz)).map(Map.Entry::getKey).findAny();
        if (classId.isPresent()) {
            return classId.get();
        }
        throw new IllegalStateException(String.format("Class %s was not registered.", clazz.getName()));
    }

    private Class<? extends IJsonSerializable> getResourceClass(String id) {
        return registeredClasses.computeIfAbsent(id, key -> {
            throw new IllegalStateException(String.format("No class with id %s was registered.", key));
        });
    }

    private void ensureFromJsonMethod() {
        for (Class<?> clazz : registeredClasses.values()) {
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
