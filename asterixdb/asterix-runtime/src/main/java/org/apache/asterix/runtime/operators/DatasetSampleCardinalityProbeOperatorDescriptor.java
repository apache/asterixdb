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

package org.apache.asterix.runtime.operators;

import static org.apache.hyracks.api.job.profiling.NoOpOperatorStats.INVALID_ODID;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChunkedComponentMetadataReaderWriter;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaEstimator;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaSampler;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * One metadata-only walk of the primary-index disk components, reporting two summed operator stats:
 *
 * <pre>
 *   tupleCounter &gt; 0  iff any component is missing the theta sketch (the walk stops at the first)
 *                    -&gt;  unbiased random sample impossible  -&gt;  caller forces FULL SCAN
 *   tupleBytes   = # storage partitions holding live data (theta cardinality &gt; 0)
 *                    -&gt;  sample-allocation divisor, ceil(target / liveCount)  (SampleOperationsHelper)
 * </pre>
 *
 * Why theta-only for the fallback: the theta sketch is the only metadata essential to a valid random
 * sample. MAX_LEAF_TUPLE_COUNT is an optional Olken bias-correction hint, legitimately absent on columnar /
 * merge / empty (fully-deleted) components, so requiring it here spuriously flipped random -&gt; full scan
 * after delete/merge cycles. Why the live-count divisor: an empty/fully-deleted partition would otherwise
 * consume a share of the target that is then dropped, leaving the sample short. Why merged into one walk: the
 * two signals share the same per-partition metadata pass instead of scanning every partition twice. Why
 * {@code tupleBytes} carries the count: there is no dedicated per-partition stat and the probe does no I/O,
 * so that counter is free to reuse. The missing-theta output is a temporary upgrade fallback -- once
 * pre-theta components are EoL, only that output (and its metadata check) is dropped; the cardinality
 * output stays.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Unified probe: live-partition count (sample allocation) + missing-metadata count (upgrade fallback) in one pass")
public final class DatasetSampleCardinalityProbeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ChunkedComponentMetadataReaderWriter THETA_SKETCH_RW =
            new ChunkedComponentMetadataReaderWriter(DiskComponentMetadata.THETA_INSERT_DELETE_SKETCH_KEY);

    private final String operatorName;
    private final IIndexDataflowHelperFactory primaryIndexHelperFactory;
    private final int[][] partitionsMap;

    public DatasetSampleCardinalityProbeOperatorDescriptor(IOperatorDescriptorRegistry spec, String operatorName,
            IIndexDataflowHelperFactory primaryIndexHelperFactory, int[][] partitionsMap) {
        super(spec, 1, 0);
        this.operatorName = operatorName;
        this.primaryIndexHelperFactory = primaryIndexHelperFactory;
        this.partitionsMap = partitionsMap;
    }

    /** Outcome of walking one storage partition's disk components. */
    private enum PartitionState {
        /** A component lacks the theta sketch -> the whole ANALYZE must fall back to a full scan. */
        MISSING_THETA,
        /** The partition holds live data (theta-estimated cardinality > 0). */
        LIVE,
        /** The partition has no disk components / no live data. */
        EMPTY
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputSinkOperatorNodePushable() {

            private long componentsMissingMetadata;
            private long livePartitions;

            @Override
            public void open() throws HyracksDataException {
                IStatsCollector coll = ctx.getStatsCollector();
                if (coll != null) {
                    coll.add(new OperatorStats(operatorName, INVALID_ODID));
                }
                INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
                probe(serviceCtx, partitionsMap[partition]);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // No input consumed; works purely off index metadata gathered in open().
            }

            @Override
            public void fail() throws HyracksDataException {
                // No-op
            }

            @Override
            public void close() throws HyracksDataException {
                IStatsCollector statsCollector = ctx.getStatsCollector();
                if (statsCollector != null) {
                    IOperatorStats stats = statsCollector.getOperatorStats(operatorName);
                    if (stats != null) {
                        stats.getTupleCounter().update(componentsMissingMetadata);
                        stats.getTupleBytes().update(livePartitions);
                    }
                }
            }

            private void probe(INCServiceContext serviceCtx, int[] partitions) throws HyracksDataException {
                int empty = 0;
                for (int p : partitions) {
                    IIndexDataflowHelper helper = primaryIndexHelperFactory.create(serviceCtx, p);
                    helper.open();
                    try {
                        switch (probePartition((ILSMIndex) helper.getIndexInstance())) {
                            case MISSING_THETA:
                                // A component predates the theta sketch, so the caller forces a full scan for the
                                // whole ANALYZE and the live-partition count (used only by the random path) is
                                // never read. Record the fallback signal and stop: no point opening the remaining
                                // partitions or estimating any more liveness.
                                componentsMissingMetadata++;
                                LOGGER.debug("probe: storage partition {} missing theta -> full-scan fallback", p);
                                return;
                            case LIVE:
                                livePartitions++;
                                break;
                            case EMPTY:
                                empty++;
                                break;
                        }
                    } finally {
                        helper.close();
                    }
                }
                // Per-task divergence source: empty>0 is exactly why the cluster-wide liveDivisor drops below
                // numPartitions (SampleOperationsHelper). Logs this NC task's slice of the partition map.
                LOGGER.debug("probe: assigned={}, live={}, empty={}", partitions.length, livePartitions, empty);
            }

            /**
             * Single walk of one partition's disk components. Returns {@code MISSING_THETA} on the first
             * component lacking the theta sketch (an unbiased random sample is then impossible cluster-wide),
             * {@code LIVE} if the partition holds live data, or {@code EMPTY} if it has no disk components.
             */
            private PartitionState probePartition(ILSMIndex index) throws HyracksDataException {
                List<ThetaEstimator.ComponentStats> componentStats = new ArrayList<>();
                ArrayBackedValueStorage thetaReference = new ArrayBackedValueStorage();
                boolean anyComponent = false;
                synchronized (index.getOperationTracker()) {
                    for (ILSMDiskComponent component : index.getDiskComponents()) {
                        anyComponent = true;
                        DiskComponentMetadata metadata = component.getMetadata();
                        boolean hasTheta = THETA_SKETCH_RW.readMetadata(metadata, thetaReference)
                                && thetaReference.getLength() > 0;
                        // Only the theta sketch is essential for an unbiased random sample. Do NOT also require
                        // MAX_LEAF_TUPLE_COUNT: that key is merely an Olken page-fill bias-correction hint,
                        // written only by some loader types (row NSM), and legitimately absent on columnar /
                        // merge / delete components. The sample cursor tolerates its absence (skips the
                        // correction when leafTupleCapacity == 0), so requiring it here spuriously forced random
                        // sampling to full scan after delete/merge cycles.
                        if (!hasTheta) {
                            // A missing theta forces a cluster-wide full-scan fallback; no need to read the rest
                            // of this partition's components (their sketches would only be discarded).
                            return PartitionState.MISSING_THETA;
                        }
                        componentStats.add(ThetaSampler.deserialize(thetaReference));
                    }
                }
                if (!anyComponent) {
                    // No disk components => empty partition (never written): contributes nothing to the sample.
                    return PartitionState.EMPTY;
                }
                return ThetaEstimator.estimatePerComponentCardinality(componentStats).totalCardinality > 0
                        ? PartitionState.LIVE : PartitionState.EMPTY;
            }
        };
    }
}
