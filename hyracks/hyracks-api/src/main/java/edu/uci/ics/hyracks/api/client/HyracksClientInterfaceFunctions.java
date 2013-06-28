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
package edu.uci.ics.hyracks.api.client;

import java.io.Serializable;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;

import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;

public class HyracksClientInterfaceFunctions {
    public enum FunctionId {
        GET_CLUSTER_CONTROLLER_INFO,
        GET_CLUSTER_TOPOLOGY,
        CREATE_JOB,
        GET_JOB_STATUS,
        START_JOB,
        GET_DATASET_DIRECTORY_SERIVICE_INFO,
        GET_DATASET_RESULT_STATUS,
        GET_DATASET_RECORD_DESCRIPTOR,
        GET_DATASET_RESULT_LOCATIONS,
        WAIT_FOR_COMPLETION,
        GET_NODE_CONTROLLERS_INFO,
        CLI_DEPLOY_BINARY,
        CLI_UNDEPLOY_BINARY
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

    public static class StartJobFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final byte[] acggfBytes;
        private final EnumSet<JobFlag> jobFlags;
        private final DeploymentId deploymentId;

        public StartJobFunction(byte[] acggfBytes, EnumSet<JobFlag> jobFlags) {
            this.acggfBytes = acggfBytes;
            this.jobFlags = jobFlags;
            this.deploymentId = null;
        }

        public StartJobFunction(DeploymentId deploymentId, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) {
            this.acggfBytes = acggfBytes;
            this.jobFlags = jobFlags;
            this.deploymentId = deploymentId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_JOB;
        }

        public byte[] getACGGFBytes() {
            return acggfBytes;
        }

        public EnumSet<JobFlag> getJobFlags() {
            return jobFlags;
        }

        public DeploymentId getDeploymentId() {
            return deploymentId;
        }
    }

    public static class GetDatasetDirectoryServiceInfoFunction extends Function {
        private static final long serialVersionUID = 1L;

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_DATASET_DIRECTORY_SERIVICE_INFO;
        }
    }

    public static class GetDatasetResultStatusFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        public GetDatasetResultStatusFunction(JobId jobId, ResultSetId rsId) {
            this.jobId = jobId;
            this.rsId = rsId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_DATASET_RESULT_STATUS;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }
    }

    public static class GetDatasetResultLocationsFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        private final DatasetDirectoryRecord[] knownRecords;

        public GetDatasetResultLocationsFunction(JobId jobId, ResultSetId rsId, DatasetDirectoryRecord[] knownRecords) {
            this.jobId = jobId;
            this.rsId = rsId;
            this.knownRecords = knownRecords;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_DATASET_RESULT_LOCATIONS;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return rsId;
        }

        public DatasetDirectoryRecord[] getKnownRecords() {
            return knownRecords;
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

        public CliDeployBinaryFunction(List<URL> binaryURLs, DeploymentId deploymentId) {
            this.binaryURLs = binaryURLs;
            this.deploymentId = deploymentId;
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
}