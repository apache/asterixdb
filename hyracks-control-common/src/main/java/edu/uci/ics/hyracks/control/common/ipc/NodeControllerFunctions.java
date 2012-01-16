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
package edu.uci.ics.hyracks.control.common.ipc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public class NodeControllerFunctions {
    public enum FunctionId {
        START_TASKS,
        ABORT_TASKS,
        CLEANUP_JOBLET,
        CREATE_APPLICATION,
        DESTROY_APPLICATION,
        REPORT_PARTITION_AVAILABILITY
    }

    public static abstract class Function implements Serializable {
        private static final long serialVersionUID = 1L;

        public abstract FunctionId getFunctionId();
    }

    public static class StartTasksFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String appName;
        private final JobId jobId;
        private final byte[] planBytes;
        private final List<TaskAttemptDescriptor> taskDescriptors;
        private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies;

        public StartTasksFunction(String appName, JobId jobId, byte[] planBytes,
                List<TaskAttemptDescriptor> taskDescriptors,
                Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies) {
            this.appName = appName;
            this.jobId = jobId;
            this.planBytes = planBytes;
            this.taskDescriptors = taskDescriptors;
            this.connectorPolicies = connectorPolicies;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_TASKS;
        }

        public String getAppName() {
            return appName;
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
    }

    public static class CreateApplicationFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String appName;
        private final boolean deployHar;
        private final byte[] serializedDistributedState;

        public CreateApplicationFunction(String appName, boolean deployHar, byte[] serializedDistributedState) {
            this.appName = appName;
            this.deployHar = deployHar;
            this.serializedDistributedState = serializedDistributedState;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CREATE_APPLICATION;
        }

        public String getAppName() {
            return appName;
        }

        public boolean isDeployHar() {
            return deployHar;
        }

        public byte[] getSerializedDistributedState() {
            return serializedDistributedState;
        }
    }

    public static class DestroyApplicationFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String appName;

        public DestroyApplicationFunction(String appName) {
            this.appName = appName;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.DESTROY_APPLICATION;
        }

        public String getAppName() {
            return appName;
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
    }
}