/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.nc.work;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.Task;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.partitions.MaterializedPartitionWriter;
import edu.uci.ics.hyracks.control.nc.partitions.MaterializingPipelinedPartition;
import edu.uci.ics.hyracks.control.nc.partitions.PipelinedPartition;
import edu.uci.ics.hyracks.control.nc.partitions.ReceiveSideMaterializingCollector;
import edu.uci.ics.hyracks.control.nc.profiling.ProfilingPartitionWriterFactory;

public class StartTasksWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(StartTasksWork.class.getName());

    private final NodeControllerService ncs;

    private final DeploymentId deploymentId;

    private final JobId jobId;

    private final byte[] acgBytes;

    private final List<TaskAttemptDescriptor> taskDescriptors;

    private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap;

    private final EnumSet<JobFlag> flags;

    public StartTasksWork(NodeControllerService ncs, DeploymentId deploymentId, JobId jobId, byte[] acgBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap, EnumSet<JobFlag> flags) {
        this.ncs = ncs;
        this.deploymentId = deploymentId;
        this.jobId = jobId;
        this.acgBytes = acgBytes;
        this.taskDescriptors = taskDescriptors;
        this.connectorPoliciesMap = connectorPoliciesMap;
        this.flags = flags;
    }

    @Override
    public void run() {
        try {
            NCApplicationContext appCtx = ncs.getApplicationContext();
            final Joblet joblet = getOrCreateLocalJoblet(deploymentId, jobId, appCtx, acgBytes == null ? null
                    : (ActivityClusterGraph) DeploymentUtils.deserialize(acgBytes, deploymentId, appCtx));
            final ActivityClusterGraph acg = joblet.getActivityClusterGraph();

            IRecordDescriptorProvider rdp = new IRecordDescriptorProvider() {
                @Override
                public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                    ActivityCluster ac = acg.getActivityMap().get(aid);
                    IConnectorDescriptor conn = ac.getActivityOutputMap().get(aid).get(outputIndex);
                    return ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                }

                @Override
                public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                    ActivityCluster ac = acg.getActivityMap().get(aid);
                    IConnectorDescriptor conn = ac.getActivityInputMap().get(aid).get(inputIndex);
                    return ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                }
            };

            for (TaskAttemptDescriptor td : taskDescriptors) {
                TaskAttemptId taId = td.getTaskAttemptId();
                TaskId tid = taId.getTaskId();
                ActivityId aid = tid.getActivityId();
                ActivityCluster ac = acg.getActivityMap().get(aid);
                IActivity han = ac.getActivityMap().get(aid);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Initializing " + taId + " -> " + han);
                }
                final int partition = tid.getPartition();
                Task task = new Task(joblet, taId, han.getClass().getName(), ncs.getExecutor(), ncs);
                IOperatorNodePushable operator = han.createPushRuntime(task, rdp, partition, td.getPartitionCount());

                List<IPartitionCollector> collectors = new ArrayList<IPartitionCollector>();

                List<IConnectorDescriptor> inputs = ac.getActivityInputMap().get(aid);
                if (inputs != null) {
                    for (int i = 0; i < inputs.size(); ++i) {
                        IConnectorDescriptor conn = inputs.get(i);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("input: " + i + ": " + conn.getConnectorId());
                        }
                        RecordDescriptor recordDesc = ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                        IPartitionCollector collector = createPartitionCollector(td, partition, task, i, conn,
                                recordDesc, cPolicy);
                        collectors.add(collector);
                    }
                }
                List<IConnectorDescriptor> outputs = ac.getActivityOutputMap().get(aid);
                if (outputs != null) {
                    for (int i = 0; i < outputs.size(); ++i) {
                        final IConnectorDescriptor conn = outputs.get(i);
                        RecordDescriptor recordDesc = ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());

                        IPartitionWriterFactory pwFactory = createPartitionWriterFactory(task, cPolicy, jobId, conn,
                                partition, taId, flags);

                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("output: " + i + ": " + conn.getConnectorId());
                        }
                        IFrameWriter writer = conn.createPartitioner(task, recordDesc, pwFactory, partition,
                                td.getPartitionCount(), td.getOutputPartitionCounts()[i]);
                        operator.setOutputFrameWriter(i, writer, recordDesc);
                    }
                }

                task.setTaskRuntime(collectors.toArray(new IPartitionCollector[collectors.size()]), operator);
                joblet.addTask(task);

                task.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Joblet getOrCreateLocalJoblet(DeploymentId deploymentId, JobId jobId, INCApplicationContext appCtx,
            ActivityClusterGraph acg) throws Exception {
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            if (acg == null) {
                throw new NullPointerException("JobActivityGraph was null");
            }
            ji = new Joblet(ncs, deploymentId, jobId, appCtx, acg);
            jobletMap.put(jobId, ji);
        }
        return ji;
    }

    private IPartitionCollector createPartitionCollector(TaskAttemptDescriptor td, final int partition, Task task,
            int i, IConnectorDescriptor conn, RecordDescriptor recordDesc, IConnectorPolicy cPolicy)
            throws HyracksDataException {
        IPartitionCollector collector = conn.createPartitionCollector(task, recordDesc, partition,
                td.getInputPartitionCounts()[i], td.getPartitionCount());
        if (cPolicy.materializeOnReceiveSide()) {
            return new ReceiveSideMaterializingCollector(task, ncs.getPartitionManager(), collector,
                    task.getTaskAttemptId(), ncs.getExecutor());
        } else {
            return collector;
        }
    }

    private IPartitionWriterFactory createPartitionWriterFactory(final IHyracksTaskContext ctx,
            IConnectorPolicy cPolicy, final JobId jobId, final IConnectorDescriptor conn, final int senderIndex,
            final TaskAttemptId taId, EnumSet<JobFlag> flags) {
        IPartitionWriterFactory factory;
        if (cPolicy.materializeOnSendSide()) {
            if (cPolicy.consumerWaitsForProducerToFinish()) {
                factory = new IPartitionWriterFactory() {
                    @Override
                    public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                        return new MaterializedPartitionWriter(ctx, ncs.getPartitionManager(), new PartitionId(jobId,
                                conn.getConnectorId(), senderIndex, receiverIndex), taId, ncs.getExecutor());
                    }
                };
            } else {
                factory = new IPartitionWriterFactory() {
                    @Override
                    public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                        return new MaterializingPipelinedPartition(ctx, ncs.getPartitionManager(), new PartitionId(
                                jobId, conn.getConnectorId(), senderIndex, receiverIndex), taId, ncs.getExecutor());
                    }
                };
            }
        } else {
            factory = new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new PipelinedPartition(ctx, ncs.getPartitionManager(), new PartitionId(jobId,
                            conn.getConnectorId(), senderIndex, receiverIndex), taId);
                }
            };
        }
        if (flags.contains(JobFlag.PROFILE_RUNTIME)) {
            factory = new ProfilingPartitionWriterFactory(ctx, conn, senderIndex, factory);
        }
        return factory;
    }
}