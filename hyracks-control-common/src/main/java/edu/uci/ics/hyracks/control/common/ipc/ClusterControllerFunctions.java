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

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;

public class ClusterControllerFunctions {
    public enum FunctionId {
        REGISTER_NODE,
        UNREGISTER_NODE,
        NOTIFY_TASK_COMPLETE,
        NOTIFY_TASK_FAILURE,
        NOTIFY_JOBLET_CLEANUP,
        NODE_HEARTBEAT,
        REPORT_PROFILE,
        REGISTER_PARTITION_PROVIDER,
        REGISTER_PARTITION_REQUEST,
        APPLICATION_STATE_CHANGE_RESPONSE,
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

        private final JobId jobId;
        private final TaskAttemptId taskId;
        private final String nodeId;
        private final TaskProfile statistics;

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
    }

    public static class NotifyTaskFailureFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final TaskAttemptId taskId;
        private final String nodeId;
        private final String details;

        public NotifyTaskFailureFunction(JobId jobId, TaskAttemptId taskId, String nodeId, String details) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.nodeId = nodeId;
            this.details = details;
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

        public String getDetails() {
            return details;
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
    }

    public static class ApplicationStateChangeResponseFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String nodeId;
        private final String appName;
        private final ApplicationStatus status;

        public ApplicationStateChangeResponseFunction(String nodeId, String appName, ApplicationStatus status) {
            this.nodeId = nodeId;
            this.appName = appName;
            this.status = status;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.APPLICATION_STATE_CHANGE_RESPONSE;
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getApplicationName() {
            return appName;
        }

        public ApplicationStatus getStatus() {
            return status;
        }
    }
}