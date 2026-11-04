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
package org.apache.asterix.metadata.utils;

import static org.apache.asterix.metadata.utils.DatasetUtil.getDatasetResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetResource;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("squid:S1181")
public class TruncateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private static final long TIMEOUT = 5;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
    private final Map<String, List<DatasetPartitions>> allDatasets;

    public TruncateOperatorDescriptor(IOperatorDescriptorRegistry spec,
            Map<String, List<DatasetPartitions>> allDatasets) {
        super(spec, 0, 0);
        this.allDatasets = allDatasets;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int part, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                INcApplicationContext appCtx =
                        (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
                INCServiceContext ctx = appCtx.getServiceContext();
                DatasetLifecycleManager dslMgr = (DatasetLifecycleManager) appCtx.getDatasetLifecycleManager();
                List<DatasetPartitions> nodeDatasets = new ArrayList<>();
                try {
                    List<DatasetPartitions> datasets = allDatasets.get(ctx.getNodeId());
                    for (DatasetPartitions dataset : datasets) {
                        nodeDatasets.add(dataset);
                        for (Integer partition : dataset.getPartitions()) {
                            IIndexDataflowHelper indexDataflowHelper =
                                    dataset.getPrimaryIndexDataflowHelperFactory().create(ctx, partition);
                            indexDataflowHelper.open();
                            try {
                                ILSMIndex index = (ILSMIndex) indexDataflowHelper.getIndexInstance();
                                // Partial Rollback
                                final LocalResource resource = indexDataflowHelper.getResource();
                                final int indexStoragePartition =
                                        DatasetResourceReference.of(resource).getPartitionNum();
                                DatasetResource dsr = getDatasetResource(dslMgr, indexStoragePartition,
                                        (IndexDataflowHelper) indexDataflowHelper);
                                dsr.getDatasetInfo().waitForIO();
                                ILSMComponentIdGenerator idGenerator = dslMgr.getComponentIdGenerator(
                                        dsr.getDatasetID(), indexStoragePartition, resource.getPath());
                                idGenerator.refresh();
                                truncate(ctx, index, dataset.getSecondaryIndexDataflowHelperFactories(), partition,
                                        idGenerator.getId());
                            } finally {
                                indexDataflowHelper.close();
                            }
                        }
                        LOGGER.info("Truncated collection {} partitions {}", dataset.getDataset(),
                                dataset.getPartitions());
                    }
                } catch (Throwable e) {
                    LOGGER.log(Level.ERROR, "Exception while truncating {}", nodeDatasets, e);
                    throw HyracksDataException.create(e);
                }
            }
        };
    }

    private static void truncate(INCServiceContext ctx, ILSMIndex primaryIndex,
            List<IIndexDataflowHelperFactory> secondaries, Integer partition, ILSMComponentId nextComponentId)
            throws HyracksDataException {
        Future<Void> truncateFuture = ctx.getControllerService().getExecutor().submit(() -> {
            INcApplicationContext appCtx = (INcApplicationContext) ctx.getApplicationContext();
            long flushLsn = appCtx.getTransactionSubsystem().getLogManager().getAppendLSN();
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, nextComponentId);
            ILSMIndexAccessor lsmAccessor = primaryIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            lsmAccessor.getOpContext().setParameters(flushMap);
            Predicate<ILSMComponent> predicate = c -> true;
            List<IIndexDataflowHelper> openedSecondaries = new ArrayList<>();
            Throwable hde = null;
            try {
                for (int j = 0; j < secondaries.size(); j++) {
                    IIndexDataflowHelper sIndexDataflowHelper = secondaries.get(j).create(ctx, partition);
                    sIndexDataflowHelper.open();
                    openedSecondaries.add(sIndexDataflowHelper);
                }
                // truncate primary
                lsmAccessor.deleteComponents(predicate);
                // truncate secondaries
                for (int j = 0; j < openedSecondaries.size(); j++) {
                    ILSMIndex sIndex = (ILSMIndex) openedSecondaries.get(j).getIndexInstance();
                    ILSMIndexAccessor sLsmAccessor = sIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    sLsmAccessor.getOpContext().setParameters(flushMap);
                    sLsmAccessor.deleteComponents(predicate);
                }
            } catch (Throwable th) {
                hde = HyracksDataException.create(th);
            } finally {
                hde = ResourceReleaseUtils.close(openedSecondaries, hde);
            }
            if (hde != null) {
                throw HyracksDataException.create(hde);
            }
            return null;
        });
        try {
            truncateFuture.get(TIMEOUT, TIMEOUT_UNIT);
        } catch (Exception e) {
            LOGGER.fatal("halting due to a failure to truncate", e);
        }
    }

}
