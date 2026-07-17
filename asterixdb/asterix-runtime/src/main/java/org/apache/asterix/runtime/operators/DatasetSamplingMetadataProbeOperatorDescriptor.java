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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;

/**
 * Probes the primary index disk components for the random-sampling metadata keys
 * ({@link DiskComponentMetadata#THETA_INSERT_DELETE_SKETCH_KEY} and
 * {@link DiskComponentMetadata#MAX_LEAF_TUPLE_COUNT_KEY}) and reports, via operator stats, the number of disk
 * components that are missing either key. The count is summed across partitions, so a total of {@code 0} means every
 * disk component (in every partition) carries the metadata required for an unbiased random sample; any positive value
 * means at least one component predates the sampling metadata (e.g., not yet merged) and the caller should fall back to
 * a full scan.
 */
public final class DatasetSamplingMetadataProbeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final IIndexDataflowHelperFactory primaryIndexHelperFactory;
    private final int[][] partitionsMap;

    public DatasetSamplingMetadataProbeOperatorDescriptor(IOperatorDescriptorRegistry spec, String operatorName,
            IIndexDataflowHelperFactory primaryIndexHelperFactory, int[][] partitionsMap) {
        super(spec, 1, 0);
        this.operatorName = operatorName;
        this.primaryIndexHelperFactory = primaryIndexHelperFactory;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputSinkOperatorNodePushable() {

            private long componentsMissingMetadata;

            @Override
            public void open() throws HyracksDataException {
                IStatsCollector coll = ctx.getStatsCollector();
                if (coll != null) {
                    coll.add(new OperatorStats(operatorName, INVALID_ODID));
                }
                INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
                componentsMissingMetadata = countComponentsMissingMetadata(serviceCtx, partitionsMap[partition]);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // No input is consumed; the probe works purely off the index metadata gathered in open().
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
                    }
                }
            }

            private long countComponentsMissingMetadata(INCServiceContext serviceCtx, int[] partitions)
                    throws HyracksDataException {
                long missing = 0;
                for (int p : partitions) {
                    IIndexDataflowHelper helper = primaryIndexHelperFactory.create(serviceCtx, p);
                    helper.open();
                    try {
                        ILSMIndex index = (ILSMIndex) helper.getIndexInstance();
                        synchronized (index.getOperationTracker()) {
                            for (ILSMDiskComponent component : index.getDiskComponents()) {
                                DiskComponentMetadata metadata = component.getMetadata();
                                if (!hasKey(metadata, DiskComponentMetadata.THETA_INSERT_DELETE_SKETCH_KEY)
                                        || !hasKey(metadata, DiskComponentMetadata.MAX_LEAF_TUPLE_COUNT_KEY)) {
                                    missing++;
                                }
                            }
                        }
                    } finally {
                        helper.close();
                    }
                }
                return missing;
            }

            private boolean hasKey(DiskComponentMetadata metadata, IValueReference key) throws HyracksDataException {
                ArrayBackedValueStorage buffer = new ArrayBackedValueStorage();
                return metadata.get(key, buffer) && buffer.getLength() > 0;
            }
        };
    }
}
