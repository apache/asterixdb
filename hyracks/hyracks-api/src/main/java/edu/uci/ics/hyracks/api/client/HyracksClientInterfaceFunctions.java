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
package edu.uci.ics.hyracks.api.client;

import java.io.Serializable;
import java.util.EnumSet;

import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;

public class HyracksClientInterfaceFunctions {
    public enum FunctionId {
        GET_CLUSTER_CONTROLLER_INFO,
        GET_CLUSTER_TOPOLOGY,
        CREATE_APPLICATION,
        START_APPLICATION,
        DESTROY_APPLICATION,
        CREATE_JOB,
        GET_JOB_STATUS,
        START_JOB,
        GET_DATASET_DIRECTORY_SERIVICE_INFO,
        GET_DATASET_RESULT_STATUS,
        GET_DATASET_RECORD_DESCRIPTOR,
        GET_DATASET_RESULT_LOCATIONS,
        WAIT_FOR_COMPLETION,
        GET_NODE_CONTROLLERS_INFO
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

    public static class CreateApplicationFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String appName;

        public CreateApplicationFunction(String appName) {
            this.appName = appName;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.CREATE_APPLICATION;
        }

        public String getAppName() {
            return appName;
        }
    }

    public static class StartApplicationFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final String appName;

        public StartApplicationFunction(String appName) {
            this.appName = appName;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_APPLICATION;
        }

        public String getAppName() {
            return appName;
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

        private final String appName;
        private final byte[] acggfBytes;
        private final EnumSet<JobFlag> jobFlags;

        public StartJobFunction(String appName, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) {
            this.appName = appName;
            this.acggfBytes = acggfBytes;
            this.jobFlags = jobFlags;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.START_JOB;
        }

        public String getAppName() {
            return appName;
        }

        public byte[] getACGGFBytes() {
            return acggfBytes;
        }

        public EnumSet<JobFlag> getJobFlags() {
            return jobFlags;
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

    public static class GetDatasetRecordDescriptorFunction extends Function {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;

        private final ResultSetId rsId;

        public GetDatasetRecordDescriptorFunction(JobId jobId, ResultSetId rsId) {
            this.jobId = jobId;
            this.rsId = rsId;
        }

        @Override
        public FunctionId getFunctionId() {
            return FunctionId.GET_DATASET_RECORD_DESCRIPTOR;
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
}