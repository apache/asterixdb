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
package org.apache.hyracks.control.nc.work;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.EnforceFrameWriter;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.comm.channels.NetworkInputChannel;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.control.nc.partitions.MaterializedPartitionWriter;
import org.apache.hyracks.control.nc.partitions.MaterializingPipelinedPartition;
import org.apache.hyracks.control.nc.partitions.PipelinedPartition;
import org.apache.hyracks.control.nc.partitions.ReceiveSideMaterializingCollector;
import org.apache.hyracks.control.nc.profiling.ProfilingPartitionWriterFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StartTasksWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final NodeControllerService ncs;

    private final DeploymentId deploymentId;

    private final JobId jobId;

    private final DeployedJobSpecId deployedJobSpecId;

    private final byte[] acgBytes;

    private final List<TaskAttemptDescriptor> taskDescriptors;

    private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap;

    private final Set<JobFlag> flags;

    private final Map<byte[], byte[]> jobParameters;

    private final long jobStartTime;

    public StartTasksWork(NodeControllerService ncs, DeploymentId deploymentId, JobId jobId, byte[] acgBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap, Set<JobFlag> flags,
            Map<byte[], byte[]> jobParameters, DeployedJobSpecId deployedJobSpecId, long jobStartTime) {
        this.ncs = ncs;
        this.deploymentId = deploymentId;
        this.jobId = jobId;
        this.deployedJobSpecId = deployedJobSpecId;
        this.acgBytes = acgBytes;
        this.taskDescriptors = taskDescriptors;
        this.connectorPoliciesMap = connectorPoliciesMap;
        this.flags = flags;
        this.jobParameters = jobParameters;
        this.jobStartTime = jobStartTime;
    }

    @Override
    public void run() {
        Task task = null;
        int taskIndex = 0;
        try {
            ncs.updateMaxJobId(jobId);
            NCServiceContext serviceCtx = ncs.getContext();
            Joblet joblet = getOrCreateLocalJoblet(deploymentId, serviceCtx, acgBytes);
            if (ncs.getNodeStatus() != NodeStatus.ACTIVE) {
                throw HyracksException.create(ErrorCode.NODE_IS_NOT_ACTIVE, ncs.getId());
            }
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
            while (taskIndex < taskDescriptors.size()) {
                TaskAttemptDescriptor td = taskDescriptors.get(taskIndex);
                TaskAttemptId taId = td.getTaskAttemptId();
                TaskId tid = taId.getTaskId();
                ActivityId aid = tid.getActivityId();
                ActivityCluster ac = acg.getActivityMap().get(aid);
                IActivity han = ac.getActivityMap().get(aid);
                LOGGER.trace("Initializing {} -> {} for {}", taId, han, jobId);
                final int partition = tid.getPartition();
                List<IConnectorDescriptor> inputs = ac.getActivityInputMap().get(aid);
                task = null;
                task = new Task(joblet, flags, taId, han.getClass().getName(), ncs.getExecutor(), ncs,
                        createInputChannels(td, inputs));
                IOperatorNodePushable operator = han.createPushRuntime(task, rdp, partition, td.getPartitionCount());
                List<IPartitionCollector> collectors = new ArrayList<>();
                if (inputs != null) {
                    for (int i = 0; i < inputs.size(); ++i) {
                        IConnectorDescriptor conn = inputs.get(i);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());
                        LOGGER.trace("input: {}: {}", i, conn.getConnectorId());
                        RecordDescriptor recordDesc = ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                        IPartitionCollector collector =
                                createPartitionCollector(td, partition, task, i, conn, recordDesc, cPolicy);
                        collectors.add(collector);
                    }
                }
                List<IConnectorDescriptor> outputs = ac.getActivityOutputMap().get(aid);
                if (outputs != null) {
                    final boolean enforce = flags.contains(JobFlag.ENFORCE_CONTRACT);
                    final boolean profile = flags.contains(JobFlag.PROFILE_RUNTIME);
                    for (int i = 0; i < outputs.size(); ++i) {
                        final IConnectorDescriptor conn = outputs.get(i);
                        RecordDescriptor recordDesc = ac.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());

                        IPartitionWriterFactory pwFactory =
                                createPartitionWriterFactory(task, cPolicy, jobId, conn, partition, taId, flags);
                        LOGGER.trace("input: {}: {}", i, conn.getConnectorId());
                        IFrameWriter writer = conn.createPartitioner(task, recordDesc, pwFactory, partition,
                                td.getPartitionCount(), td.getOutputPartitionCounts()[i]);
                        writer = (enforce && !profile) ? EnforceFrameWriter.enforce(writer) : writer;
                        operator.setOutputFrameWriter(i, writer, recordDesc);
                    }
                }

                task.setTaskRuntime(collectors.toArray(new IPartitionCollector[collectors.size()]), operator);
                joblet.addTask(task);
                task.start();
                taskIndex++;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure starting a task", e);
            // notify cc of start task failure
            List<Exception> exceptions = new ArrayList<>();
            exceptions.add(e);
            ExceptionUtils.setNodeIds(exceptions, ncs.getId());
            TaskAttemptId taskId = taskDescriptors.get(taskIndex).getTaskAttemptId();
            ncs.getWorkQueue().schedule(new NotifyTaskFailureWork(ncs, task, exceptions, jobId, taskId));
        }
    }

    private Joblet getOrCreateLocalJoblet(DeploymentId deploymentId, INCServiceContext appCtx, byte[] acgBytes)
            throws HyracksException {
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            ActivityClusterGraph acg = (deployedJobSpecId != null) ? ncs.getActivityClusterGraph(deployedJobSpecId)
                    : (ActivityClusterGraph) DeploymentUtils.deserialize(acgBytes, deploymentId, appCtx);
            ncs.createOrGetJobParameterByteStore(jobId).setParameters(jobParameters);
            IJobletEventListenerFactory listenerFactory = acg.getJobletEventListenerFactory();
            if (listenerFactory != null) {
                if (deployedJobSpecId != null) {
                    listenerFactory = acg.getJobletEventListenerFactory().copyFactory();
                }
                listenerFactory.updateListenerJobParameters(ncs.createOrGetJobParameterByteStore(jobId));
            }
            ji = new Joblet(ncs, deploymentId, jobId, appCtx, acg, listenerFactory, jobStartTime);
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
            final TaskAttemptId taId, Set<JobFlag> flags) {
        IPartitionWriterFactory factory;
        if (cPolicy.materializeOnSendSide()) {
            if (cPolicy.consumerWaitsForProducerToFinish()) {
                factory = new IPartitionWriterFactory() {
                    @Override
                    public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                        return new MaterializedPartitionWriter(ctx, ncs.getPartitionManager(),
                                new PartitionId(jobId, conn.getConnectorId(), senderIndex, receiverIndex), taId,
                                ncs.getExecutor());
                    }
                };
            } else {
                factory = new IPartitionWriterFactory() {

                    @Override
                    public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                        return new MaterializingPipelinedPartition(ctx, ncs.getPartitionManager(),
                                new PartitionId(jobId, conn.getConnectorId(), senderIndex, receiverIndex), taId,
                                ncs.getExecutor());
                    }
                };
            }
        } else {
            factory = new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new PipelinedPartition(ctx, ncs.getPartitionManager(),
                            new PartitionId(jobId, conn.getConnectorId(), senderIndex, receiverIndex), taId);
                }
            };
        }
        if (flags.contains(JobFlag.PROFILE_RUNTIME)) {
            factory = new ProfilingPartitionWriterFactory(ctx, conn, senderIndex, factory);
        }
        return factory;
    }

    /**
     * Create a list of known channels for each input connector
     *
     * @param td
     *            the task attempt id
     * @param inputs
     *            the input connector descriptors
     * @return a list of known channels, one for each connector
     * @throws UnknownHostException
     */
    private List<List<PartitionChannel>> createInputChannels(TaskAttemptDescriptor td,
            List<IConnectorDescriptor> inputs) throws UnknownHostException {
        NetworkAddress[][] inputAddresses = td.getInputPartitionLocations();
        List<List<PartitionChannel>> channelsForInputConnectors = new ArrayList<>();
        if (inputAddresses != null) {
            for (int i = 0; i < inputAddresses.length; i++) {
                List<PartitionChannel> channels = new ArrayList<>();
                if (inputAddresses[i] != null) {
                    for (int j = 0; j < inputAddresses[i].length; j++) {
                        NetworkAddress networkAddress = inputAddresses[i][j];
                        PartitionId pid = new PartitionId(jobId, inputs.get(i).getConnectorId(), j,
                                td.getTaskAttemptId().getTaskId().getPartition());
                        PartitionChannel channel = new PartitionChannel(pid,
                                new NetworkInputChannel(ncs.getNetworkManager(),
                                        new InetSocketAddress(
                                                InetAddress.getByAddress(networkAddress.lookupIpAddress()),
                                                networkAddress.getPort()),
                                        pid, 5));
                        channels.add(channel);
                    }
                }
                channelsForInputConnectors.add(channels);
            }
        }
        return channelsForInputConnectors;
    }
}
