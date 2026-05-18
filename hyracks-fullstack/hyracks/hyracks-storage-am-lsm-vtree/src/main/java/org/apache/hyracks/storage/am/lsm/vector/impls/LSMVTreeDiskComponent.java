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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * LSM disk component for Vector Clustering Trees. Wraps a materialized {@link VTree} on persistent
 * storage; the wrapped tree may have been produced by a memory-component flush, by initial
 * bulk-load at CREATE INDEX, or by a merge.
 */
public class LSMVTreeDiskComponent extends AbstractLSMDiskComponent {

    // Operation-parameter keys recognized by createBulkLoader(). These are written into the
    // operation's parameter map by the upstream operator (e.g. the static-structure creator) and
    // dispatched on in createBulkLoader.
    public static final String PARAM_NUM_LEVELS = "numLevels";
    public static final String PARAM_CLUSTERS_PER_LEVEL = "clustersPerLevel";
    public static final String PARAM_CENTROIDS_PER_CLUSTER = "centroidsPerCluster";
    public static final String PARAM_MAX_ENTRIES_PER_PAGE = "maxEntriesPerPage";
    public static final String PARAM_STATIC_STRUCTURE_COMPONENT = "static_structure_component";

    private final VTree vTree;

    /**
     * Marks this component as carrying the index's static structure (root + interior + leaf pages,
     * built once at CREATE INDEX time). Set by {@code LSMVTree.createStaticStructure} so that
     * {@code LSMVTree.addBulkLoadedDiskComponent} can dispatch the just-loaded component to the
     * static-structure slot instead of into the regular disk-component list.
     */
    private boolean isStaticStructure = false;

    public LSMVTreeDiskComponent(AbstractLSMIndex lsmIndex, VTree vTree, ILSMComponentFilter filter) {
        super(lsmIndex, getMetadataPageManager(vTree), filter);
        this.vTree = vTree;
    }

    @Override
    public VTree getIndex() {
        return vTree;
    }

    @Override
    public VTree getMetadataHolder() {
        return vTree;
    }

    @Override
    public long getComponentSize() {
        return getComponentSize(vTree);
    }

    @Override
    public int getFileReferenceCount() {
        return getFileReferenceCount(vTree);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return getFiles(vTree);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + vTree.getFileReference().getRelativePath();
    }

    @Override
    public void validate() throws HyracksDataException {
        vTree.validate();
    }

    /**
     * A static-structure build is signalled by the presence of the clustering-shape descriptor in the
     * bulk-load parameters (canonically {@link #PARAM_NUM_LEVELS}; the producer always supplies the full set
     * — {@link #PARAM_CLUSTERS_PER_LEVEL} / {@link #PARAM_CENTROIDS_PER_CLUSTER} /
     * {@link #PARAM_MAX_ENTRIES_PER_PAGE} — which {@link #createVTreeStaticStructureBulkLoader} then
     * validates). Any other bulk load is a data load.
     */
    public static boolean isStaticStructureLoad(Map<String, Object> parameters) {
        return parameters.containsKey(PARAM_NUM_LEVELS);
    }

    /**
     * Creates a bulk loader for VTree with static structure support.
     *
     * Follows the LSMBTree chained-bulk-loader pattern. If the operation parameters carry the
     * static-structure descriptor ({@link #isStaticStructureLoad}), a static-structure loader is wired in;
     * otherwise a regular data-loading bulk loader is used, sourcing centroids from the pre-built static
     * structure component.
     */
    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(NCConfig storageConfig, ILSMIOOperation operation,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {

        ChainedLSMDiskComponentBulkLoader chainedBulkLoader =
                new ChainedLSMDiskComponentBulkLoader(operation, this, cleanupEmptyComponent);

        // Add filter bulk loader if needed
        if (withFilter && getLsmIndex().getFilterFields() != null) {
            chainedBulkLoader.addBulkLoader(createFilterBulkLoader());
        }

        // Check if this is a static structure bulk load operation
        Map<String, Object> parameters = operation.getParameters();
        boolean isStaticStructureLoad = isStaticStructureLoad(parameters);
        IChainedComponentBulkLoader indexBulkLoader = isStaticStructureLoad
                ? createVTreeStaticStructureBulkLoader(operation, callback) : createVTreeBulkLoader(operation);

        chainedBulkLoader.addBulkLoader(indexBulkLoader);
        callback.initialize(chainedBulkLoader);

        return chainedBulkLoader;
    }

    /**
     * Creates a VTreeStaticStructureLoader configured from the operation parameters
     * ({@link #PARAM_NUM_LEVELS} / {@link #PARAM_CLUSTERS_PER_LEVEL} /
     * {@link #PARAM_CENTROIDS_PER_CLUSTER} / {@link #PARAM_MAX_ENTRIES_PER_PAGE}) and wraps it as an
     * {@link IChainedComponentBulkLoader}.
     */
    private IChainedComponentBulkLoader createVTreeStaticStructureBulkLoader(ILSMIOOperation operation,
            IPageWriteCallback callback) throws HyracksDataException {

        // These are required parameters: every producer (the static-structure creator operator and the
        // test harness) always supplies all four, and the caller only invokes this method when the three
        // structural keys are present. Fail loudly if any is missing/null rather than silently building the
        // index with an arbitrary default clustering shape (which would corrupt the static structure).
        Map<String, Object> parameters = operation.getParameters();
        Integer numLevelsParam = (Integer) parameters.get(PARAM_NUM_LEVELS);
        @SuppressWarnings("unchecked")
        List<Integer> clustersPerLevel = (List<Integer>) parameters.get(PARAM_CLUSTERS_PER_LEVEL);
        @SuppressWarnings("unchecked")
        List<List<Integer>> centroidsPerCluster = (List<List<Integer>>) parameters.get(PARAM_CENTROIDS_PER_CLUSTER);
        Integer maxEntriesParam = (Integer) parameters.get(PARAM_MAX_ENTRIES_PER_PAGE);
        if (numLevelsParam == null || clustersPerLevel == null || centroidsPerCluster == null
                || maxEntriesParam == null) {
            throw new HyracksDataException("static-structure bulk load requires non-null " + PARAM_NUM_LEVELS + ", "
                    + PARAM_CLUSTERS_PER_LEVEL + ", " + PARAM_CENTROIDS_PER_CLUSTER + ", and "
                    + PARAM_MAX_ENTRIES_PER_PAGE + " parameters");
        }
        int numLevels = numLevelsParam;
        int maxEntriesPerPage = maxEntriesParam;

        IIndexBulkLoader ssbuilder = getIndex().createStaticStructureBulkLoader(numLevels, clustersPerLevel,
                centroidsPerCluster, maxEntriesPerPage, callback);
        return new LSMIndexBulkLoader(ssbuilder, getMetadata(), getSampler());
    }

    /**
     * Creates a regular data-loading bulk loader that uses the already-built static structure
     * component (passed via the {@link #PARAM_STATIC_STRUCTURE_COMPONENT} parameter) to assign
     * incoming records to leaf clusters.
     */
    private IChainedComponentBulkLoader createVTreeBulkLoader(ILSMIOOperation operation) throws HyracksDataException {
        Object staticParam = operation.getParameters().get(PARAM_STATIC_STRUCTURE_COMPONENT);
        if (!(staticParam instanceof LSMVTreeDiskComponent staticComponent)) {
            throw new HyracksDataException(
                    PARAM_STATIC_STRUCTURE_COMPONENT + " must be provided in parameters for data loading");
        }
        ITreeIndexAccessor staticAccessor =
                staticComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        // Merges drain tuples from the merge cursor, which may hand over PRESERVED antimatter
        // tuples (returnDeletedTuples=true when the merging set excludes the oldest disk
        // component). The tree's default data frames use the insert (matter) tuple writer, which
        // would silently re-encode preserved antimatter as matter and resurrect deletes — so
        // merges write through an antimatter-preserving copy tuple writer instead (LSMBTree's
        // copy-tuple-writer pattern). Initial bulk load (LOAD) keeps the default matter writer.
        ITreeIndexFrameFactory dataFrameFactoryOverride =
                operation.getIOOperationType() == LSMIOOperationType.MERGE ? createCopyDataFrameFactory() : null;
        IIndexBulkLoader builder = getIndex().createComponentBulkLoader(NoOpPageWriteCallback.INSTANCE, staticAccessor,
                getSampler(), dataFrameFactoryOverride);
        return new LSMIndexBulkLoader(builder, getMetadata(), getSampler());
    }

    /**
     * Builds a data-frame factory identical to the tree's default except that its tuple writer
     * carries over the source tuple's antimatter bit.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    private ITreeIndexFrameFactory createCopyDataFrameFactory() {
        VTreeDataFrameFactory dataFrameFactory = (VTreeDataFrameFactory) getIndex().getDataFrameFactory();
        LSMVTreeDataTupleWriterFactory tupleWriterFactory =
                (LSMVTreeDataTupleWriterFactory) dataFrameFactory.getTupleWriterFactory();
        return new VTreeDataFrameFactory(tupleWriterFactory.createCopyWriterFactory(),
                dataFrameFactory.getVectorDimensions());
    }

    static IMetadataPageManager getMetadataPageManager(VTree vTree) {
        return (IMetadataPageManager) vTree.getPageManager();
    }

    static long getComponentSize(VTree vTree) {
        return vTree.getFileReference().getFile().length();
    }

    static int getFileReferenceCount(VTree vTree) {
        return vTree.getBufferCache().getFileReferenceCount(vTree.getFileId());
    }

    static Set<String> getFiles(VTree vTree) {
        Set<String> files = new HashSet<>();
        files.add(vTree.getFileReference().getFile().getAbsolutePath());
        return files;
    }

    public boolean isStaticStructure() {
        return isStaticStructure;
    }

    public void setStaticStructure(boolean isStaticStructure) {
        this.isStaticStructure = isStaticStructure;
    }

    public void setInitialized() throws HyracksDataException {
        int rootPageId = vTree.getPageManager().getRootPageId();
        vTree.setRootPageId(rootPageId);
    }
}
