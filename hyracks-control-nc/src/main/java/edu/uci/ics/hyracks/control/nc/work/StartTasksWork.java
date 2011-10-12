/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.Task;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.partitions.MaterializedPartitionWriter;
import edu.uci.ics.hyracks.control.nc.partitions.PipelinedPartition;
import edu.uci.ics.hyracks.control.nc.partitions.ReceiveSideMaterializingCollector;

public class StartTasksWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(StartTasksWork.class.getName());

    private final NodeControllerService ncs;

    private final String appName;

    private final JobId jobId;

    private final byte[] jagBytes;

    private final List<TaskAttemptDescriptor> taskDescriptors;

    private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap;

    public StartTasksWork(NodeControllerService ncs, String appName, JobId jobId, byte[] jagBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap) {
        this.ncs = ncs;
        this.appName = appName;
        this.jobId = jobId;
        this.jagBytes = jagBytes;
        this.taskDescriptors = taskDescriptors;
        this.connectorPoliciesMap = connectorPoliciesMap;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            Map<String, NCApplicationContext> applications = ncs.getApplications();
            NCApplicationContext appCtx = applications.get(appName);
            final JobActivityGraph plan = (JobActivityGraph) appCtx.deserialize(jagBytes);

            IRecordDescriptorProvider rdp = new IRecordDescriptorProvider() {
                @Override
                public RecordDescriptor getOutputRecordDescriptor(OperatorDescriptorId opId, int outputIndex) {
                    return plan.getJobSpecification().getOperatorOutputRecordDescriptor(opId, outputIndex);
                }

                @Override
                public RecordDescriptor getInputRecordDescriptor(OperatorDescriptorId opId, int inputIndex) {
                    return plan.getJobSpecification().getOperatorInputRecordDescriptor(opId, inputIndex);
                }
            };

            final Joblet joblet = getOrCreateLocalJoblet(jobId, appCtx);

            for (TaskAttemptDescriptor td : taskDescriptors) {
                TaskAttemptId taId = td.getTaskAttemptId();
                TaskId tid = taId.getTaskId();
                IActivity han = plan.getActivityNodeMap().get(tid.getActivityId());
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Initializing " + taId + " -> " + han);
                }
                final int partition = tid.getPartition();
                Task task = new Task(joblet, taId, han.getClass().getName(), ncs.getExecutor());
                IOperatorEnvironment env = joblet.getEnvironment(tid.getActivityId().getOperatorDescriptorId(),
                        tid.getPartition());
                IOperatorNodePushable operator = han.createPushRuntime(task, env, rdp, partition,
                        td.getPartitionCount());

                List<IPartitionCollector> collectors = new ArrayList<IPartitionCollector>();

                List<IConnectorDescriptor> inputs = plan.getActivityInputConnectorDescriptors(tid.getActivityId());
                if (inputs != null) {
                    for (int i = 0; i < inputs.size(); ++i) {
                        IConnectorDescriptor conn = inputs.get(i);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("input: " + i + ": " + conn.getConnectorId());
                        }
                        RecordDescriptor recordDesc = plan.getJobSpecification().getConnectorRecordDescriptor(conn);
                        IPartitionCollector collector = createPartitionCollector(td, partition, task, i, conn,
                                recordDesc, cPolicy);
                        collectors.add(collector);
                    }
                }
                List<IConnectorDescriptor> outputs = plan.getActivityOutputConnectorDescriptors(tid.getActivityId());
                if (outputs != null) {
                    for (int i = 0; i < outputs.size(); ++i) {
                        final IConnectorDescriptor conn = outputs.get(i);
                        RecordDescriptor recordDesc = plan.getJobSpecification().getConnectorRecordDescriptor(conn);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());

                        IPartitionWriterFactory pwFactory = createPartitionWriterFactory(cPolicy, jobId, conn,
                                partition, taId);

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
            throw e;
        }
    }

    private Joblet getOrCreateLocalJoblet(JobId jobId, INCApplicationContext appCtx) throws Exception {
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            ji = new Joblet(ncs, jobId, appCtx);
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
            return new ReceiveSideMaterializingCollector(ncs.getRootContext(), ncs.getPartitionManager(), collector,
                    task.getTaskAttemptId(), ncs.getExecutor());
        } else {
            return collector;
        }
    }

    private IPartitionWriterFactory createPartitionWriterFactory(IConnectorPolicy cPolicy, final JobId jobId,
            final IConnectorDescriptor conn, final int senderIndex, final TaskAttemptId taId) {
        if (cPolicy.materializeOnSendSide()) {
            return new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new MaterializedPartitionWriter(ncs.getRootContext(), ncs.getPartitionManager(),
                            new PartitionId(jobId, conn.getConnectorId(), senderIndex, receiverIndex), taId,
                            ncs.getExecutor());
                }
            };
        } else {
            return new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new PipelinedPartition(ncs.getPartitionManager(), new PartitionId(jobId,
                            conn.getConnectorId(), senderIndex, receiverIndex), taId);
                }
            };
        }
    }
}