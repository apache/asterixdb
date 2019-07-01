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
package org.apache.hyracks.api.client;

import java.io.Serializable;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultSetId;

public class HyracksClientInterfaceFunctions {
    public enum FunctionId {
        GET_CLUSTER_CONTROLLER_INFO,
        GET_CLUSTER_TOPOLOGY,
        GET_JOB_STATUS,
        GET_JOB_INFO,
        START_JOB,
        DEPLOY_JOB,
        UNDEPLOY_JOB,
        REDEPLOY_JOB,
        CANCEL_JOB,
        GET_RESULT_DIRECTORY_ADDRESS,
        GET_RESULT_STATUS,
        GET_RESULT_LOCATIONS,
        GET_RESULT_METADATA,
        WAIT_FOR_COMPLETION,
        GET_NODE_CONTROLLERS_INFO,
        CLI_DEPLOY_BINARY,
        CLI_UNDEPLOY_BINARY,
        CLUSTER_SHUTDOWN,
        GET_NODE_DETAILS_JSON,
        THREAD_DUMP
    }

    public abstract static class Function implements Serializable {
        private static final long serialVersionUID = 1L;

        public abstract FunctionId getFunctionId();
    }

    public static class GetClusterControllerInfoFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_CLUSTER_CONTROLLER_INFO;
        }
    }

    public static class GetJobStatusFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        public GetJobStatusFunction(JobId jobId) {
            this.jobId = jobId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_JOB_STATUS;
        }

        public JobId getJobId() {
            return jobId;
        }
    }

    public static class GetJobInfoFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        public GetJobInfoFunction(JobId jobId) {
            this.jobId = jobId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_JOB_INFO;
        }

        public JobId getJobId() {
            return jobId;
        }
    }

    public static class redeployJobSpecFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final byte[] acggfBytes;

        private final DeployedJobSpecId deployedJobSpecId;

        public redeployJobSpecFunction(DeployedJobSpecId deployedJobSpecId, byte[] acggfBytes) {
            this.deployedJobSpecId = deployedJobSpecId;
            this.acggfBytes = acggfBytes;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.REDEPLOY_JOB;
        }

        public byte[] getACGGFBytes() {
            return acggfBytes;
        }

        public DeployedJobSpecId getDeployedJobSpecId() {
            return deployedJobSpecId;
        }
    }

    public static class DeployJobSpecFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final byte[] acggfBytes;

        public DeployJobSpecFunction(byte[] acggfBytes) {
            this.acggfBytes = acggfBytes;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.DEPLOY_JOB;
        }

        public byte[] getACGGFBytes() {
            return acggfBytes;
        }
    }

    public static class CancelJobFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        public CancelJobFunction(JobId jobId) {
            this.jobId = jobId;
            if (jobId == null) {
                throw new IllegalArgumentException("jobId");
            }
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CANCEL_JOB;
        }

        public JobId getJobId() {
            return jobId;
        }
    }

    public static class UndeployJobSpecFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final DeployedJobSpecId deployedJobSpecId;

        public UndeployJobSpecFunction(DeployedJobSpecId deployedJobSpecId) {
            this.deployedJobSpecId = deployedJobSpecId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.UNDEPLOY_JOB;
        }

        public DeployedJobSpecId getDeployedJobSpecId() {
            return deployedJobSpecId;
        }
    }

    public static class StartJobFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final byte[] acggfBytes;
        private final Set<JobFlag> jobFlags;
        private final DeploymentId deploymentId;
        private final DeployedJobSpecId deployedJobSpecId;
        private final Map<byte[], byte[]> jobParameters;

        public StartJobFunction(DeploymentId deploymentId, byte[] acggfBytes, Set<JobFlag> jobFlags,
                DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) {
            this.acggfBytes = acggfBytes;
            this.jobFlags = jobFlags;
            this.deploymentId = deploymentId;
            this.deployedJobSpecId = deployedJobSpecId;
            this.jobParameters = jobParameters;
        }

        public StartJobFunction(DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) {
            this(null, null, EnumSet.noneOf(JobFlag.class), deployedJobSpecId, jobParameters);
        }

        public StartJobFunction(byte[] acggfBytes, Set<JobFlag> jobFlags) {
            this(null, acggfBytes, jobFlags, null, null);
        }

        public StartJobFunction(DeploymentId deploymentId, byte[] acggfBytes, Set<JobFlag> jobFlags) {
            this(deploymentId, acggfBytes, jobFlags, null, null);
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_JOB;
        }

        public Map<byte[], byte[]> getJobParameters() {
            return jobParameters;
        }

        public DeployedJobSpecId getDeployedJobSpecId() {
            return deployedJobSpecId;
        }

        public byte[] getACGGFBytes() {
            return acggfBytes;
        }

        public Set<JobFlag> getJobFlags() {
            return jobFlags;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }
    }

    public static class GetResultDirectoryAddressFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_RESULT_DIRECTORY_ADDRESS;
        }
    }

    public static class GetResultStatusFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        public GetResultStatusFunction(JobId jobId, ResultSetId rsId) {
            this.jobId = jobId;
            this.rsId = rsId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_RESULT_STATUS;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }
    }

    public static class GetResultLocationsFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        private final ResultDirectoryRecord[] knownRecords;

        public GetResultLocationsFunction(JobId jobId, ResultSetId rsId, ResultDirectoryRecord[] knownRecords) {
            this.jobId = jobId;
            this.rsId = rsId;
            this.knownRecords = knownRecords;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_RESULT_LOCATIONS;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }

        public ResultDirectoryRecord[] getKnownRecords() {
            return knownRecords;
        }
    }

    public static class GetResultMetadataFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        public GetResultMetadataFunction(JobId jobId, ResultSetId rsId) {
            this.jobId = jobId;
            this.rsId = rsId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_RESULT_METADATA;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }
    }

    public static class WaitForCompletionFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        public WaitForCompletionFunction(JobId jobId) {
            this.jobId = jobId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.WAIT_FOR_COMPLETION;
        }

        public JobId getJobId() {
            return jobId;
        }
    }

    public static class GetNodeControllersInfoFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_NODE_CONTROLLERS_INFO;
        }
    }

    public static class GetClusterTopologyFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_CLUSTER_TOPOLOGY;
        }
    }

    public static class CliDeployBinaryFunction extends Function {
        private static final long serialVersionUID = 1L;
        private final List<URL> binaryURLs;
        private final DeploymentId deploymentId;
        private final boolean extractFromArchive;

        public CliDeployBinaryFunction(List<URL> binaryURLs, DeploymentId deploymentId, boolean isExtractFromArchive) {
            this.binaryURLs = binaryURLs;
            this.deploymentId = deploymentId;
            this.extractFromArchive = isExtractFromArchive;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CLI_DEPLOY_BINARY;
        }

        public List<URL> getBinaryURLs() {
            return binaryURLs;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }

        public boolean isExtractFromArchive() {
            return extractFromArchive;
        }
    }

    public static class CliUnDeployBinaryFunction extends Function {
        private static final long serialVersionUID = 1L;
        private final DeploymentId deploymentId;

        public CliUnDeployBinaryFunction(DeploymentId deploymentId) {
            this.deploymentId = deploymentId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CLI_UNDEPLOY_BINARY;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }
    }

    public static class ClusterShutdownFunction extends Function {
        private static final long serialVersionUID = 1L;
        private final boolean terminateNCService;

        public ClusterShutdownFunction(boolean terminateNCService) {
            this.terminateNCService = terminateNCService;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CLUSTER_SHUTDOWN;
        }

        public boolean isTerminateNCService() {
            return terminateNCService;
        }
    }

    public static class GetNodeDetailsJSONFunction extends Function {
        private static final long serialVersionUID = 1L;
        private final String nodeId;
        private final boolean includeStats;
        private final boolean includeConfig;

        public GetNodeDetailsJSONFunction(String nodeId, boolean includeStats, boolean includeConfig) {
            this.nodeId = nodeId;
            this.includeStats = includeStats;
            this.includeConfig = includeConfig;
        }

        public String getNodeId() {
            return nodeId;
        }

        public boolean isIncludeStats() {
            return includeStats;
        }

        public boolean isIncludeConfig() {
            return includeConfig;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_NODE_DETAILS_JSON;
        }
    }

    public static class ThreadDumpFunction extends Function {
        private static final long serialVersionUID = 2956155746070390274L;
        private final String node;

        public ThreadDumpFunction(String node) {
            this.node = node;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.THREAD_DUMP;
        }

        public String getNode() {
            return node;
        }
    }
}
