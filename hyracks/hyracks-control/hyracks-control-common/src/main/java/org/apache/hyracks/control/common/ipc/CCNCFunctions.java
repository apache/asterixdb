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
package org.apache.hyracks.control.common.ipc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.connectors.ConnectorPolicyFactory;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;

public class CCNCFunctions {
    private static final Logger LOGGER = Logger.getLogger(CCNCFunctions.class.getName());

    private static final int FID_CODE_SIZE = 1;

    public enum FunctionId {
        REGISTER_NODE,
        UNREGISTER_NODE,
        NOTIFY_JOBLET_CLEANUP,
        NOTIFY_TASK_COMPLETE,
        NOTIFY_TASK_FAILURE,
        NODE_HEARTBEAT,
        REPORT_PROFILE,
        REGISTER_PARTITION_PROVIDER,
        REGISTER_PARTITION_REQUEST,
        REGISTER_RESULT_PARTITION_LOCATION,
        REPORT_RESULT_PARTITION_WRITE_COMPLETION,
        REPORT_RESULT_PARTITION_FAILURE,

        NODE_REGISTRATION_RESULT,
        START_TASKS,
        ABORT_TASKS,
        CLEANUP_JOBLET,
        REPORT_PARTITION_AVAILABILITY,
        SEND_APPLICATION_MESSAGE,
        GET_NODE_CONTROLLERS_INFO,
        GET_NODE_CONTROLLERS_INFO_RESPONSE,

        DEPLOY_BINARY,
        NOTIFY_DEPLOY_BINARY,
        UNDEPLOY_BINARY,
        SHUTDOWN_REQUEST,
        SHUTDOWN_RESPONSE,

        STATE_DUMP_REQUEST,
        STATE_DUMP_RESPONSE,

        OTHER
    }

    public static class SendApplicationMessageFunction extends Function {
        private static final long serialVersionUID = 1L;
        private byte[] serializedMessage;
        private DeploymentId deploymentId;
        private String nodeId;

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public byte[] getMessage() {
            return serializedMessage;
        }

        public SendApplicationMessageFunction(byte[] data, DeploymentId deploymentId, String nodeId) {
            this.serializedMessage = data;
            this.deploymentId = deploymentId;
            this.nodeId = nodeId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.SEND_APPLICATION_MESSAGE;
        }

    }

    public static abstract class Function implements Serializable {
        private static final long serialVersionUID = 1L;

        public abstract FunctionId getFunctionId();
    }

    public static class RegisterNodeFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final NodeRegistration reg;

        public RegisterNodeFunction(NodeRegistration reg) {
            this.reg = reg;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REGISTER_NODE;
        }

        public NodeRegistration getNodeRegistration() {
            return reg;
        }
    }

    public static class UnregisterNodeFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;

        public UnregisterNodeFunction(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.UNREGISTER_NODE;
        }

        public String getNodeId() {
            return nodeId;
        }
    }

    public static class NotifyTaskCompleteFunction extends Function {
        private static final long serialVersionUID = 1L;

        private JobId jobId;
        private TaskAttemptId taskId;
        private String nodeId;
        private TaskProfile statistics;

        public NotifyTaskCompleteFunction(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.nodeId = nodeId;
            this.statistics = statistics;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NOTIFY_TASK_COMPLETE;
        }

        public JobId getJobId() {
            return jobId;
        }

        public TaskAttemptId getTaskId() {
            return taskId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public TaskProfile getStatistics() {
            return statistics;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            JobId jobId = JobId.create(dis);
            String nodeId = dis.readUTF();
            TaskAttemptId taskId = TaskAttemptId.create(dis);
            TaskProfile statistics = TaskProfile.create(dis);
            return new NotifyTaskCompleteFunction(jobId, taskId, nodeId, statistics);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            NotifyTaskCompleteFunction fn = (NotifyTaskCompleteFunction) object;
            DataOutputStream dos = new DataOutputStream(out);
            fn.jobId.writeFields(dos);
            dos.writeUTF(fn.nodeId);
            fn.taskId.writeFields(dos);
            fn.statistics.writeFields(dos);
        }
    }

    public static class NotifyTaskFailureFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final TaskAttemptId taskId;
        private final String nodeId;
        private final List<Exception> exceptions;

        public NotifyTaskFailureFunction(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.nodeId = nodeId;
            this.exceptions = exceptions;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NOTIFY_TASK_FAILURE;
        }

        public JobId getJobId() {
            return jobId;
        }

        public TaskAttemptId getTaskId() {
            return taskId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public List<Exception> getExceptions() {
            return exceptions;
        }
    }

    public static class NotifyJobletCleanupFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final String nodeId;

        public NotifyJobletCleanupFunction(JobId jobId, String nodeId) {
            this.jobId = jobId;
            this.nodeId = nodeId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NOTIFY_JOBLET_CLEANUP;
        }

        public JobId getJobId() {
            return jobId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            JobId jobId = JobId.create(dis);
            String nodeId = dis.readUTF();

            return new NotifyJobletCleanupFunction(jobId, nodeId);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            NotifyJobletCleanupFunction fn = (NotifyJobletCleanupFunction) object;
            DataOutputStream dos = new DataOutputStream(out);
            fn.jobId.writeFields(dos);
            dos.writeUTF(fn.nodeId);
        }
    }

    public static class NodeHeartbeatFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;
        private final HeartbeatData hbData;

        public NodeHeartbeatFunction(String nodeId, HeartbeatData hbData) {
            this.nodeId = nodeId;
            this.hbData = hbData;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NODE_HEARTBEAT;
        }

        public String getNodeId() {
            return nodeId;
        }

        public HeartbeatData getHeartbeatData() {
            return hbData;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            String nodeId = dis.readUTF();
            HeartbeatData hbData = new HeartbeatData();
            hbData.readFields(dis);
            return new NodeHeartbeatFunction(nodeId, hbData);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            NodeHeartbeatFunction fn = (NodeHeartbeatFunction) object;
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(fn.nodeId);
            fn.hbData.write(dos);
        }
    }

    public static class ReportProfileFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;
        private final List<JobProfile> profiles;

        public ReportProfileFunction(String nodeId, List<JobProfile> profiles) {
            this.nodeId = nodeId;
            this.profiles = profiles;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REPORT_PROFILE;
        }

        public String getNodeId() {
            return nodeId;
        }

        public List<JobProfile> getProfiles() {
            return profiles;
        }
    }

    public static class RegisterPartitionProviderFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final PartitionDescriptor partitionDescriptor;

        public RegisterPartitionProviderFunction(PartitionDescriptor partitionDescriptor) {
            this.partitionDescriptor = partitionDescriptor;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REGISTER_PARTITION_PROVIDER;
        }

        public PartitionDescriptor getPartitionDescriptor() {
            return partitionDescriptor;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            // Read PartitionId
            PartitionId pid = readPartitionId(dis);

            // Read nodeId
            String nodeId = dis.readUTF();

            // Read TaskAttemptId
            TaskAttemptId taId = readTaskAttemptId(dis);

            // Read reusable flag
            boolean reusable = dis.readBoolean();

            // Read Partition State
            PartitionState state = readPartitionState(dis);

            PartitionDescriptor pd = new PartitionDescriptor(pid, nodeId, taId, reusable);
            pd.setState(state);
            return new RegisterPartitionProviderFunction(pd);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            RegisterPartitionProviderFunction fn = (RegisterPartitionProviderFunction) object;

            DataOutputStream dos = new DataOutputStream(out);

            PartitionDescriptor pd = fn.getPartitionDescriptor();

            // Write PartitionId
            writePartitionId(dos, pd.getPartitionId());

            // Write nodeId
            dos.writeUTF(pd.getNodeId());

            // Write TaskAttemptId
            writeTaskAttemptId(dos, pd.getProducingTaskAttemptId());

            // Write reusable flag
            dos.writeBoolean(pd.isReusable());

            // Write Partition State
            writePartitionState(dos, pd.getState());
        }
    }

    public static class RegisterPartitionRequestFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final PartitionRequest partitionRequest;

        public RegisterPartitionRequestFunction(PartitionRequest partitionRequest) {
            this.partitionRequest = partitionRequest;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REGISTER_PARTITION_REQUEST;
        }

        public PartitionRequest getPartitionRequest() {
            return partitionRequest;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            // Read PartitionId
            PartitionId pid = readPartitionId(dis);

            // Read nodeId
            String nodeId = dis.readUTF();

            // Read TaskAttemptId
            TaskAttemptId taId = readTaskAttemptId(dis);

            // Read Partition State
            PartitionState state = readPartitionState(dis);

            PartitionRequest pr = new PartitionRequest(pid, nodeId, taId, state);
            return new RegisterPartitionRequestFunction(pr);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            RegisterPartitionRequestFunction fn = (RegisterPartitionRequestFunction) object;

            DataOutputStream dos = new DataOutputStream(out);

            PartitionRequest pr = fn.getPartitionRequest();

            // Write PartitionId
            writePartitionId(dos, pr.getPartitionId());

            // Write nodeId
            dos.writeUTF(pr.getNodeId());

            // Write TaskAttemptId
            writeTaskAttemptId(dos, pr.getRequestingTaskAttemptId());

            // Write Partition State
            writePartitionState(dos, pr.getMinimumState());
        }
    }

    public static class RegisterResultPartitionLocationFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        private final boolean orderedResult;

        private final boolean emptyResult;

        private final int partition;

        private final int nPartitions;

        private NetworkAddress networkAddress;

        public RegisterResultPartitionLocationFunction(JobId jobId, ResultSetId rsId, boolean orderedResult,
                boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress) {
            this.jobId = jobId;
            this.rsId = rsId;
            this.orderedResult = orderedResult;
            this.emptyResult = emptyResult;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.networkAddress = networkAddress;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REGISTER_RESULT_PARTITION_LOCATION;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }

        public boolean getOrderedResult() {
            return orderedResult;
        }

        public boolean getEmptyResult() {
            return emptyResult;
        }

        public int getPartition() {
            return partition;
        }

        public int getNPartitions() {
            return nPartitions;
        }

        public NetworkAddress getNetworkAddress() {
            return networkAddress;
        }
    }

    public static class ReportResultPartitionWriteCompletionFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        private final int partition;

        public ReportResultPartitionWriteCompletionFunction(JobId jobId, ResultSetId rsId, int partition) {
            this.jobId = jobId;
            this.rsId = rsId;
            this.partition = partition;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REPORT_RESULT_PARTITION_WRITE_COMPLETION;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }

        public int getPartition() {
            return partition;
        }
    }

    public static class ReportResultPartitionFailureFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        private final int partition;

        public ReportResultPartitionFailureFunction(JobId jobId, ResultSetId rsId, int partition) {
            this.jobId = jobId;
            this.rsId = rsId;
            this.partition = partition;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REPORT_RESULT_PARTITION_FAILURE;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }

        public int getPartition() {
            return partition;
        }
    }

    public static class NodeRegistrationResult extends Function {
        private static final long serialVersionUID = 1L;

        private final NodeParameters params;

        private final Exception exception;

        public NodeRegistrationResult(NodeParameters params, Exception exception) {
            this.params = params;
            this.exception = exception;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NODE_REGISTRATION_RESULT;
        }

        public NodeParameters getNodeParameters() {
            return params;
        }

        public Exception getException() {
            return exception;
        }
    }

    public static class StartTasksFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final DeploymentId deploymentId;
        private final JobId jobId;
        private final byte[] planBytes;
        private final List<TaskAttemptDescriptor> taskDescriptors;
        private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies;
        private final EnumSet<JobFlag> flags;

        public StartTasksFunction(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
                List<TaskAttemptDescriptor> taskDescriptors,
                Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies, EnumSet<JobFlag> flags) {
            this.deploymentId = deploymentId;
            this.jobId = jobId;
            this.planBytes = planBytes;
            this.taskDescriptors = taskDescriptors;
            this.connectorPolicies = connectorPolicies;
            this.flags = flags;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_TASKS;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }

        public JobId getJobId() {
            return jobId;
        }

        public byte[] getPlanBytes() {
            return planBytes;
        }

        public List<TaskAttemptDescriptor> getTaskDescriptors() {
            return taskDescriptors;
        }

        public Map<ConnectorDescriptorId, IConnectorPolicy> getConnectorPolicies() {
            return connectorPolicies;
        }

        public EnumSet<JobFlag> getFlags() {
            return flags;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            //read jobId and taskId
            JobId jobId = JobId.create(dis);
            DeploymentId deploymentId = null;
            boolean hasDeployed = dis.readBoolean();
            if (hasDeployed) {
                deploymentId = DeploymentId.create(dis);
            }

            // read plan bytes
            int planBytesSize = dis.readInt();
            byte[] planBytes = null;
            if (planBytesSize >= 0) {
                planBytes = new byte[planBytesSize];
                dis.read(planBytes, 0, planBytesSize);
            }

            // read task attempt descriptors
            int tadSize = dis.readInt();
            List<TaskAttemptDescriptor> taskDescriptors = new ArrayList<TaskAttemptDescriptor>();
            for (int i = 0; i < tadSize; i++) {
                TaskAttemptDescriptor tad = TaskAttemptDescriptor.create(dis);
                taskDescriptors.add(tad);
            }

            //read connector policies
            int cpSize = dis.readInt();
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();
            for (int i = 0; i < cpSize; i++) {
                ConnectorDescriptorId cid = ConnectorDescriptorId.create(dis);
                IConnectorPolicy policy = ConnectorPolicyFactory.INSTANCE.getConnectorPolicy(dis);
                connectorPolicies.put(cid, policy);
            }

            // read flags
            int flagSize = dis.readInt();
            EnumSet<JobFlag> flags = EnumSet.noneOf(JobFlag.class);
            for (int i = 0; i < flagSize; i++) {
                flags.add(JobFlag.values()[(dis.readInt())]);
            }

            return new StartTasksFunction(deploymentId, jobId, planBytes, taskDescriptors, connectorPolicies, flags);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            StartTasksFunction fn = (StartTasksFunction) object;
            DataOutputStream dos = new DataOutputStream(out);

            //write jobId and deploymentId
            fn.jobId.writeFields(dos);
            dos.writeBoolean(fn.deploymentId == null ? false : true);
            if (fn.deploymentId != null) {
                fn.deploymentId.writeFields(dos);
            }

            //write plan bytes
            dos.writeInt(fn.planBytes == null ? -1 : fn.planBytes.length);
            if (fn.planBytes != null) {
                dos.write(fn.planBytes, 0, fn.planBytes.length);
            }

            //write task descriptors
            dos.writeInt(fn.taskDescriptors.size());
            for (int i = 0; i < fn.taskDescriptors.size(); i++) {
                fn.taskDescriptors.get(i).writeFields(dos);
            }

            //write connector policies
            dos.writeInt(fn.connectorPolicies.size());
            for (Entry<ConnectorDescriptorId, IConnectorPolicy> entry : fn.connectorPolicies.entrySet()) {
                entry.getKey().writeFields(dos);
                ConnectorPolicyFactory.INSTANCE.writeConnectorPolicy(entry.getValue(), dos);
            }

            //write flags
            dos.writeInt(fn.flags.size());
            for (JobFlag flag : fn.flags) {
                dos.writeInt(flag.ordinal());
            }
        }
    }

    public static class AbortTasksFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final List<TaskAttemptId> tasks;

        public AbortTasksFunction(JobId jobId, List<TaskAttemptId> tasks) {
            this.jobId = jobId;
            this.tasks = tasks;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.ABORT_TASKS;
        }

        public JobId getJobId() {
            return jobId;
        }

        public List<TaskAttemptId> getTasks() {
            return tasks;
        }
    }

    public static class CleanupJobletFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final JobStatus status;

        public CleanupJobletFunction(JobId jobId, JobStatus status) {
            this.jobId = jobId;
            this.status = status;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CLEANUP_JOBLET;
        }

        public JobId getJobId() {
            return jobId;
        }

        public JobStatus getStatus() {
            return status;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            JobId jobId = JobId.create(dis);
            JobStatus status = JobStatus.values()[dis.readInt()];

            return new CleanupJobletFunction(jobId, status);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            CleanupJobletFunction fn = (CleanupJobletFunction) object;
            DataOutputStream dos = new DataOutputStream(out);
            fn.jobId.writeFields(dos);
            dos.writeInt(fn.status.ordinal());
        }
    }

    public static class GetNodeControllersInfoFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_NODE_CONTROLLERS_INFO;
        }
    }

    public static class GetNodeControllersInfoResponseFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final Map<String, NodeControllerInfo> ncInfos;

        public GetNodeControllersInfoResponseFunction(Map<String, NodeControllerInfo> ncInfos) {
            this.ncInfos = ncInfos;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_NODE_CONTROLLERS_INFO_RESPONSE;
        }

        public Map<String, NodeControllerInfo> getNodeControllerInfos() {
            return ncInfos;
        }
    }

    public static class ReportPartitionAvailabilityFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final PartitionId pid;
        private final NetworkAddress networkAddress;

        public ReportPartitionAvailabilityFunction(PartitionId pid, NetworkAddress networkAddress) {
            this.pid = pid;
            this.networkAddress = networkAddress;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REPORT_PARTITION_AVAILABILITY;
        }

        public PartitionId getPartitionId() {
            return pid;
        }

        public NetworkAddress getNetworkAddress() {
            return networkAddress;
        }

        public static Object deserialize(ByteBuffer buffer, int length) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array(), buffer.position(), length);
            DataInputStream dis = new DataInputStream(bais);

            // Read PartitionId
            PartitionId pid = readPartitionId(dis);

            // Read NetworkAddress
            NetworkAddress networkAddress = readNetworkAddress(dis);

            return new ReportPartitionAvailabilityFunction(pid, networkAddress);
        }

        public static void serialize(OutputStream out, Object object) throws Exception {
            ReportPartitionAvailabilityFunction fn = (ReportPartitionAvailabilityFunction) object;

            DataOutputStream dos = new DataOutputStream(out);

            // Write PartitionId
            writePartitionId(dos, fn.getPartitionId());

            // Write NetworkAddress
            writeNetworkAddress(dos, fn.getNetworkAddress());
        }
    }

    public static class DeployBinaryFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final List<URL> binaryURLs;
        private final DeploymentId deploymentId;

        public DeployBinaryFunction(DeploymentId deploymentId, List<URL> binaryURLs) {
            this.binaryURLs = binaryURLs;
            this.deploymentId = deploymentId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.DEPLOY_BINARY;
        }

        public List<URL> getBinaryURLs() {
            return binaryURLs;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }
    }

    public static class UnDeployBinaryFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final DeploymentId deploymentId;

        public UnDeployBinaryFunction(DeploymentId deploymentId) {
            this.deploymentId = deploymentId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.UNDEPLOY_BINARY;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }
    }

    public static class RequestShutdownFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.SHUTDOWN_REQUEST;
        }

    }

    public static class NotifyShutdownFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;

        public NotifyShutdownFunction(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.SHUTDOWN_RESPONSE;
        }

        public String getNodeId() {
            return nodeId;
        }

    }

    public static class NotifyDeployBinaryFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;
        private final DeploymentId deploymentId;
        private final DeploymentStatus deploymentStatus;

        public NotifyDeployBinaryFunction(DeploymentId deploymentId, String nodeId, DeploymentStatus deploymentStatus) {
            this.nodeId = nodeId;
            this.deploymentId = deploymentId;
            this.deploymentStatus = deploymentStatus;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.NOTIFY_DEPLOY_BINARY;
        }

        public String getNodeId() {
            return nodeId;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }

        public DeploymentStatus getDeploymentStatus() {
            return deploymentStatus;
        }
    }

    public static class StateDumpRequestFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String stateDumpId;

        public StateDumpRequestFunction(String stateDumpId) {
            this.stateDumpId = stateDumpId;
        }

        public String getStateDumpId() {
            return stateDumpId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.STATE_DUMP_REQUEST;
        }

    }

    public static class StateDumpResponseFunction extends Function {

        private static final long serialVersionUID = 1L;

        private final String nodeId;

        private final String stateDumpId;

        private final String state;

        public StateDumpResponseFunction(String nodeId, String stateDumpId, String state) {
            this.nodeId = nodeId;
            this.stateDumpId = stateDumpId;
            this.state = state;
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getStateDumpId() {
            return stateDumpId;
        }

        public String getState() {
            return state;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.STATE_DUMP_RESPONSE;
        }
    }

    public static class ShutdownRequestFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.SHUTDOWN_REQUEST;
        }
    }

    public static class ShutdownResponseFunction extends Function {

        private final String nodeId;

        public ShutdownResponseFunction(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.SHUTDOWN_RESPONSE;
        }
    }

    public static class SerializerDeserializer implements IPayloadSerializerDeserializer {
        private final JavaSerializationBasedPayloadSerializerDeserializer javaSerde;

        public SerializerDeserializer() {
            javaSerde = new JavaSerializationBasedPayloadSerializerDeserializer();
        }

        @Override
        public Object deserializeObject(ByteBuffer buffer, int length) throws Exception {
            if (length < FID_CODE_SIZE) {
                throw new IllegalStateException("Message size too small: " + length);
            }
            byte fid = buffer.get();
            return deserialize(fid, buffer, length - FID_CODE_SIZE);
        }

        @Override
        public Exception deserializeException(ByteBuffer buffer, int length) throws Exception {
            if (length < FID_CODE_SIZE) {
                throw new IllegalStateException("Message size too small: " + length);
            }
            byte fid = buffer.get();
            if (fid != FunctionId.OTHER.ordinal()) {
                throw new IllegalStateException("Expected FID for OTHER, found: " + fid);
            }
            return (Exception) deserialize(fid, buffer, length - FID_CODE_SIZE);
        }

        @Override
        public byte[] serializeObject(Object object) throws Exception {
            if (object instanceof Function) {
                Function fn = (Function) object;
                return serialize(object, (byte) fn.getFunctionId().ordinal());
            } else {
                return serialize(object, (byte) FunctionId.OTHER.ordinal());
            }
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            return serialize(object, (byte) FunctionId.OTHER.ordinal());
        }

        private byte[] serialize(Object object, byte fid) throws Exception {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(fid);
            try {
                serialize(baos, object, fid);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error serializing " + object, e);
                throw e;
            }
            baos.close();
            return baos.toByteArray();
        }

        private void serialize(OutputStream out, Object object, byte fid) throws Exception {
            switch (FunctionId.values()[fid]) {
                case REGISTER_PARTITION_PROVIDER:
                    RegisterPartitionProviderFunction.serialize(out, object);
                    return;

                case REGISTER_PARTITION_REQUEST:
                    RegisterPartitionRequestFunction.serialize(out, object);
                    return;

                case REPORT_PARTITION_AVAILABILITY:
                    ReportPartitionAvailabilityFunction.serialize(out, object);
                    return;

                case NODE_HEARTBEAT:
                    NodeHeartbeatFunction.serialize(out, object);
                    return;

                case START_TASKS:
                    StartTasksFunction.serialize(out, object);
                    return;

                case NOTIFY_TASK_COMPLETE:
                    NotifyTaskCompleteFunction.serialize(out, object);
                    return;

                case NOTIFY_JOBLET_CLEANUP:
                    NotifyJobletCleanupFunction.serialize(out, object);
                    return;
                case CLEANUP_JOBLET:
                    CleanupJobletFunction.serialize(out, object);
                    return;
            }
            JavaSerializationBasedPayloadSerializerDeserializer.serialize(out, object);
        }

        private Object deserialize(byte fid, ByteBuffer buffer, int length) throws Exception {
            switch (FunctionId.values()[fid]) {
                case REGISTER_PARTITION_PROVIDER:
                    return RegisterPartitionProviderFunction.deserialize(buffer, length);

                case REGISTER_PARTITION_REQUEST:
                    return RegisterPartitionRequestFunction.deserialize(buffer, length);

                case REPORT_PARTITION_AVAILABILITY:
                    return ReportPartitionAvailabilityFunction.deserialize(buffer, length);

                case NODE_HEARTBEAT:
                    return NodeHeartbeatFunction.deserialize(buffer, length);

                case START_TASKS:
                    return StartTasksFunction.deserialize(buffer, length);

                case NOTIFY_TASK_COMPLETE:
                    return NotifyTaskCompleteFunction.deserialize(buffer, length);

                case NOTIFY_JOBLET_CLEANUP:
                    return NotifyJobletCleanupFunction.deserialize(buffer, length);

                case CLEANUP_JOBLET:
                    return CleanupJobletFunction.deserialize(buffer, length);
            }

            return javaSerde.deserializeObject(buffer, length);
        }
    }

    private static PartitionId readPartitionId(DataInputStream dis) throws IOException {
        long jobId = dis.readLong();
        int cdid = dis.readInt();
        int senderIndex = dis.readInt();
        int receiverIndex = dis.readInt();
        PartitionId pid = new PartitionId(new JobId(jobId), new ConnectorDescriptorId(cdid), senderIndex, receiverIndex);
        return pid;
    }

    private static void writePartitionId(DataOutputStream dos, PartitionId pid) throws IOException {
        dos.writeLong(pid.getJobId().getId());
        dos.writeInt(pid.getConnectorDescriptorId().getId());
        dos.writeInt(pid.getSenderIndex());
        dos.writeInt(pid.getReceiverIndex());
    }

    private static TaskAttemptId readTaskAttemptId(DataInputStream dis) throws IOException {
        int odid = dis.readInt();
        int aid = dis.readInt();
        int partition = dis.readInt();
        int attempt = dis.readInt();
        TaskAttemptId taId = new TaskAttemptId(new TaskId(new ActivityId(new OperatorDescriptorId(odid), aid),
                partition), attempt);
        return taId;
    }

    private static void writeTaskAttemptId(DataOutputStream dos, TaskAttemptId taId) throws IOException {
        TaskId tid = taId.getTaskId();
        ActivityId aid = tid.getActivityId();
        OperatorDescriptorId odId = aid.getOperatorDescriptorId();
        dos.writeInt(odId.getId());
        dos.writeInt(aid.getLocalId());
        dos.writeInt(tid.getPartition());
        dos.writeInt(taId.getAttempt());
    }

    private static PartitionState readPartitionState(DataInputStream dis) throws IOException {
        PartitionState state = PartitionState.values()[dis.readInt()];
        return state;
    }

    private static void writePartitionState(DataOutputStream dos, PartitionState state) throws IOException {
        dos.writeInt(state.ordinal());
    }

    private static NetworkAddress readNetworkAddress(DataInputStream dis) throws IOException {
        String address = dis.readUTF();
        int port = dis.readInt();
        NetworkAddress networkAddress = new NetworkAddress(address, port);
        return networkAddress;
    }

    private static void writeNetworkAddress(DataOutputStream dos, NetworkAddress networkAddress) throws IOException {
        dos.writeUTF(networkAddress.getAddress());
        dos.writeInt(networkAddress.getPort());
    }
}
