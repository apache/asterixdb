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

package org.apache.hyracks.storage.am.lsm.vector.util;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVTreeUtils;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.TestVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Test context for LSM Vector Clustering Tree tests.
 * This class provides the infrastructure needed to test LSMVTree operations,
 * following the established patterns of other LSM tree test contexts.
 */
@SuppressWarnings("rawtypes")
public final class LSMVTreeTestContext extends AbstractVectorTreeTestContext {

    public LSMVTreeTestContext(ISerializerDeserializer[] fieldSerdes, LSMVTree lsmVTree, int vectorDimensions)
            throws HyracksDataException {
        super(fieldSerdes, lsmVTree, false, vectorDimensions);
    }

    @Override
    public int getKeyFieldCount() {
        LSMVTree lsmTree = (LSMVTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMVTree lsmTree = (LSMVTree) index;
        return lsmTree.getComparatorFactories();
    }

    /**
     * Create a new LSMVTreeTestContext with the specified parameters.
     * Uses the default (standard) data tuple creator factory.
     */
    public static LSMVTreeTestContext create(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) throws Exception {

        return create(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, fieldSerdes,
                numVectorFields, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, null);
    }

    /**
     * Create a new LSMVTreeTestContext with a custom data tuple creator factory.
     *
     * @param dataTupleBuilderFactory the factory to use for creating data tuples,
     *                                or null to use the default (standard) factory
     */
    public static LSMVTreeTestContext create(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory) throws Exception {

        return create(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, fieldSerdes,
                numVectorFields, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, dataTupleBuilderFactory, 0);
    }

    /**
     * Create a new LSMVTreeTestContext with include fields support.
     *
     * @param numIncludeFields number of include fields in the data record (0 = none)
     */
    public static LSMVTreeTestContext create(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, int numIncludeFields) throws Exception {

        return create(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, fieldSerdes,
                numVectorFields, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, null, numIncludeFields);
    }

    /**
     * Create a new LSMVTreeTestContext with a custom data tuple creator factory and include fields.
     *
     * @param dataTupleBuilderFactory the factory to use for creating data tuples,
     *                                or null to use the default (standard) factory
     * @param numIncludeFields number of include fields in the data record (0 = none)
     */
    public static LSMVTreeTestContext create(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, int numIncludeFields) throws Exception {

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);

        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            } else if (fieldSerdes[i] instanceof DoubleSerializerDeserializer) {
                cmpFactories[i] = DoubleBinaryComparatorFactory.INSTANCE;
            } else {
                cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
            }
        }

        // Test fixtures always use the non-quantized data-tuple creator (quantization is verified
        // separately by the quantized-* test suite which constructs its own factory).
        IVTreeDataTupleBuilderFactory effectiveFactory = dataTupleBuilderFactory != null ? dataTupleBuilderFactory
                : new VTreeDataTupleBuilderFactory(numIncludeFields, false);
        LSMVTree lsmVTree = LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, file,
                diskBufferCache, typeTraits, cmpFactories, 0.0, // bloomFilterFalsePositiveRate
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, numVectorFields, // vectorDimensions
                new int[] { 0 }, // vectorFields
                (int[]) null, // filterFields
                (ILSMComponentFilterFrameFactory) null, // filterFrameFactory
                (LSMComponentFilterManager) null, // filterManager
                (IComponentFilterHelper) null, // filterHelper
                true, // durable
                metadataPageManagerFactory, false, // atomic
                (RecordDescriptor) null, TestDoubleArrayVectorAccessor.Factory.INSTANCE, // inputRecDesc, vectorAccessorFactory
                1, numIncludeFields, // numPrimaryKeyFields, numIncludeFields
                effectiveFactory, (VTreeQuantizationParams) null, // builderFactory, quantizer
                TestVTreeDistanceFunctionFactory.INSTANCE); // distanceFunctionFactory (test fixture)

        return new LSMVTreeTestContext(fieldSerdes, lsmVTree, numVectorFields);
    }

    /**
     * Build a context whose static-structure leaf frame uses the quantized (neighbor-capable) layout
     * {@code <cid, centroid, quantizedBytes, neighborList, childPtr>}. The data path stays
     * non-quantized; {@code quantizationParams} only needs to be non-null to select the quantized leaf
     * schema (its contents are consumed lazily at search/data time, which neighbor tests do not reach).
     */
    public static LSMVTreeTestContext createWithQuantizedLeafFrame(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, VTreeQuantizationParams quantizationParams)
            throws Exception {

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);

        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            } else if (fieldSerdes[i] instanceof DoubleSerializerDeserializer) {
                cmpFactories[i] = DoubleBinaryComparatorFactory.INSTANCE;
            } else {
                cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
            }
        }

        int numIncludeFields = 0;
        IVTreeDataTupleBuilderFactory effectiveFactory = new VTreeDataTupleBuilderFactory(numIncludeFields, false);
        LSMVTree lsmVTree = LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, file,
                diskBufferCache, typeTraits, cmpFactories, 0.0, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, numVectorFields, new int[] { 0 }, (int[]) null,
                (ILSMComponentFilterFrameFactory) null, (LSMComponentFilterManager) null, (IComponentFilterHelper) null,
                true, metadataPageManagerFactory, false, (RecordDescriptor) null,
                TestDoubleArrayVectorAccessor.Factory.INSTANCE, 1, numIncludeFields, effectiveFactory,
                quantizationParams, TestVTreeDistanceFunctionFactory.INSTANCE);

        return new LSMVTreeTestContext(fieldSerdes, lsmVTree, numVectorFields);
    }
}
