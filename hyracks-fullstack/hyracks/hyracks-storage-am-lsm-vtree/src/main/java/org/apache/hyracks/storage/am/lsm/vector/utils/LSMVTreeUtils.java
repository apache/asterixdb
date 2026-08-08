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

package org.apache.hyracks.storage.am.lsm.vector.utils;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeFileManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.VTreeFactory;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory helper that wires the four frame factories, the {@link VTreeFactory}, and the LSM glue
 * (file manager, disk-component factory) into a configured {@link LSMVTree}.
 */
public final class LSMVTreeUtils {

    /**
     * Field count threaded into {@link VTreeFactory}'s {@code AbstractTreeIndex} base. Matches the
     * production (quantized) leaf-tuple field count {@code <cid, centroid, qEmbed, pointer>}.
     */
    private static final int TREE_INDEX_FIELD_COUNT = 4;

    private LSMVTreeUtils() {
    }

    /**
     * Assert that the search key comparators cover {@code <distance, PK…>} — i.e. that there is a comparator
     * for field 0 and for each of the {@code numPrimaryKeyFields} fields starting at {@code pkStartField}.
     * <p>
     * Both vector search cursors build their ordering / antimatter-cancellation key from exactly those
     * positions, and both derive "how many PK fields can I compare" as
     * {@code min(comparators.length - pkStartField, numPrimaryKeyFields)}. A short comparator array therefore
     * does not fail — it silently shortens the key, and a zero-length key is worse than an error:
     * ordering collapses to distance-only (so pairwise antimatter cancellation can fire against an unrelated
     * same-distance row) and "same primary key" becomes universally true (so group reconciliation keeps only
     * the newest tuple of an equal-distance group and drops the rest). Production wiring satisfies the
     * precondition; this turns a future mis-wiring into a hard failure instead of wrong results.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    public static void validateKeyComparators(MultiComparator cmp, int pkStartField, int numPrimaryKeyFields)
            throws HyracksDataException {
        int available = cmp == null ? 0 : cmp.getComparators().length;
        if (available < pkStartField + numPrimaryKeyFields) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "VTree search key comparators too few: " + available + " comparators, but the <distance, PK> key "
                            + "needs " + (pkStartField + numPrimaryKeyFields) + " (pkStartField=" + pkStartField
                            + ", numPrimaryKeyFields=" + numPrimaryKeyFields + ")");
        }
    }

    /**
     * Build a configured {@link LSMVTree} from the per-resource configuration. Constructs the four
     * frame factories (interior/leaf/metadata pick up their fixed schemas internally; data takes
     * the caller-supplied {@code typeTraits}), the matter/antimatter
     * {@link LSMVTreeDataTupleWriterFactory} pair, and the {@link VTreeFactory} used for disk
     * components, then hands them to {@link LSMVTree}'s constructor.
     *
     * @param quantizationParams {@code float[6]} sample-file params (see
     *                           {@code IVTreeQuantizerFactory#createQuantizer}) or {@code null} for
     *                           the non-quantized test-fixture path; controls whether the leaf
     *                           frame uses the 4-field quantized layout or the 3-field bare layout.
     */
    public static LSMVTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int vectorDimensions, int[] vectorFields, int[] filterFields,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            IComponentFilterHelper filterHelper, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean atomic, RecordDescriptor inputRecDesc,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, VTreeQuantizationParams quantizationParams,
            IVTreeDistanceFunctionFactory distanceFunctionFactory) throws HyracksDataException {
        // Legacy single-closest placement; production overload below threads the real config.
        return createLSMTree(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, typeTraits,
                cmpFactories, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, vectorDimensions, vectorFields, filterFields, filterFrameFactory,
                filterManager, filterHelper, durable, metadataPageManagerFactory, atomic, inputRecDesc,
                vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields, dataTupleBuilderFactory,
                quantizationParams, distanceFunctionFactory, CrossPollinationConfig.LEGACY);
    }

    public static LSMVTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int vectorDimensions, int[] vectorFields, int[] filterFields,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            IComponentFilterHelper filterHelper, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean atomic, RecordDescriptor inputRecDesc,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, VTreeQuantizationParams quantizationParams,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination)
            throws HyracksDataException {

        // VTree tuples contain no field that is both fixed-length AND nullable, so the null bitmap is never
        // load-bearing and no INullIntrospector is needed (unlike BTree/RTree secondary keys):
        //  - the interior/leaf/metadata frames have fixed internal schemas whose fields are never null;
        //  - data-row include fields that can be NULL/MISSING are always ADM-tagged variable-length (nullable
        //    or optional types resolve to a UNION/ANY var-len trait), so a null self-encodes as a 1-byte
        //    NULL/MISSING tag in the value itself and the reader never consults the bitmap for them;
        //  - fixed-length data fields (distance/centroidId/quantized/pk) are always present and non-null.
        ITypeTraits nullTypeTraits = null;
        INullIntrospector nullIntrospector = null;
        boolean quantized = quantizationParams != null;

        // Interior, leaf, and metadata frames have fixed tuple schemas that are intrinsic to the
        // VTree design (not user-parameterized). Each factory owns its own schema and builds its
        // tuple writer internally — see the corresponding *FrameFactory class for the layout.
        ITreeIndexFrameFactory interiorFrameFactory =
                new VTreeInteriorFrameFactory(vectorDimensions, nullTypeTraits, nullIntrospector);
        ITreeIndexFrameFactory leafFrameFactory =
                new VTreeLeafFrameFactory(vectorDimensions, quantized, nullTypeTraits, nullIntrospector);
        ITreeIndexFrameFactory metadataFrameFactory =
                new VTreeMetadataFrameFactory(vectorDimensions, nullTypeTraits, nullIntrospector);

        // Data frames are caller-parameterized: typeTraits carries the ADM-tagged type traits for
        // all data-row fields (computed by VTreeResourceFactoryProvider in production, by the test
        // harness for fixtures). Production quantized layout:
        //   [distance, qDist, qEmbed, centroidId, pk, include_fields...]
        // INSERT operations use matter tuples, DELETE operations use antimatter tuples (LSMBTree
        // pattern). Disk components are immutable and only ever take insert tuples.
        LSMVTreeDataTupleWriterFactory insertDataTupleWriterFactory =
                new LSMVTreeDataTupleWriterFactory(typeTraits, false, nullTypeTraits, nullIntrospector);
        LSMVTreeDataTupleWriterFactory deleteDataTupleWriterFactory =
                new LSMVTreeDataTupleWriterFactory(typeTraits, true, nullTypeTraits, nullIntrospector);
        ITreeIndexFrameFactory insertDataFrameFactory =
                new VTreeDataFrameFactory(insertDataTupleWriterFactory, vectorDimensions);
        ITreeIndexFrameFactory deleteDataFrameFactory =
                new VTreeDataFrameFactory(deleteDataTupleWriterFactory, vectorDimensions);

        VTreeFactory vtreeFactory = new VTreeFactory(ioManager, diskBufferCache, metadataPageManagerFactory,
                interiorFrameFactory, leafFrameFactory, metadataFrameFactory, insertDataFrameFactory, cmpFactories,
                TREE_INDEX_FIELD_COUNT, vectorDimensions, vectorAccessorFactory, dataTupleBuilderFactory,
                quantizationParams, distanceFunctionFactory, crossPollination);
        ILSMIndexFileManager fileManager = new LSMVTreeFileManager(ioManager, file, vtreeFactory);
        ILSMDiskComponentFactory componentFactory = new LSMVTreeDiskComponentFactory(vtreeFactory, filterHelper);

        return new LSMVTree(storageConfig, ioManager, virtualBufferCaches, interiorFrameFactory, leafFrameFactory,
                metadataFrameFactory, insertDataFrameFactory, deleteDataFrameFactory, diskBufferCache, fileManager,
                componentFactory, componentFactory, filterHelper, filterFrameFactory, filterManager,
                bloomFilterFalsePositiveRate, cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, vectorDimensions, vectorFields, filterFields, durable, atomic,
                vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields, dataTupleBuilderFactory,
                quantizationParams, distanceFunctionFactory, crossPollination);
    }

}
