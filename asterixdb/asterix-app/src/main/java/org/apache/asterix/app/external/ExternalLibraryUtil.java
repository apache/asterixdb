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
package org.apache.asterix.app.external;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.operators.LibraryDeployAbortOperatorDescriptor;
import org.apache.asterix.external.operators.LibraryDeployCommitOperatorDescriptor;
import org.apache.asterix.external.operators.LibraryDeployPrepareOperatorDescriptor;
import org.apache.asterix.external.operators.LibraryUndeployOperatorDescriptor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class ExternalLibraryUtil {

    private ExternalLibraryUtil() {
    }

    public static Triple<JobSpecification, JobSpecification, JobSpecification> buildCreateLibraryJobSpec(
            DataverseName dataverseName, String libraryName, ExternalFunctionLanguage language, URI downloadURI,
            String authToken, MetadataProvider metadataProvider) {

        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = getSplitsAndConstraints(appCtx);

        JobSpecification prepareJobSpec = createLibraryPrepareJobSpec(dataverseName, libraryName, language, downloadURI,
                authToken, appCtx, splitsAndConstraint);

        JobSpecification commitJobSpec =
                createLibraryCommitJobSpec(dataverseName, libraryName, appCtx, splitsAndConstraint);

        JobSpecification abortJobSpec =
                createLibraryAbortJobSpec(dataverseName, libraryName, appCtx, splitsAndConstraint);

        return new Triple<>(prepareJobSpec, commitJobSpec, abortJobSpec);
    }

    private static JobSpecification createLibraryPrepareJobSpec(DataverseName dataverseName, String libraryName,
            ExternalFunctionLanguage language, URI downloadURI, String authToken, ICcApplicationContext appCtx,
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint) {
        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(appCtx);
        IOperatorDescriptor opDesc = new LibraryDeployPrepareOperatorDescriptor(jobSpec, dataverseName, libraryName,
                language, downloadURI, authToken);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc,
                splitsAndConstraint.second);
        jobSpec.addRoot(opDesc);
        return jobSpec;
    }

    private static JobSpecification createLibraryCommitJobSpec(DataverseName dataverseName, String libraryName,
            ICcApplicationContext appCtx, Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint) {
        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(appCtx);
        IOperatorDescriptor opDesc = new LibraryDeployCommitOperatorDescriptor(jobSpec, dataverseName, libraryName);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc,
                splitsAndConstraint.second);
        return jobSpec;
    }

    private static JobSpecification createLibraryAbortJobSpec(DataverseName dataverseName, String libraryName,
            ICcApplicationContext appCtx, Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint) {
        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(appCtx);
        IOperatorDescriptor opDesc = new LibraryDeployAbortOperatorDescriptor(jobSpec, dataverseName, libraryName);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc,
                splitsAndConstraint.second);
        return jobSpec;
    }

    public static JobSpecification buildDropLibraryJobSpec(DataverseName dataverseName, String libraryName,
            MetadataProvider metadataProvider) {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = getSplitsAndConstraints(appCtx);

        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(appCtx);
        IOperatorDescriptor opDesc = new LibraryUndeployOperatorDescriptor(jobSpec, dataverseName, libraryName);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc,
                splitsAndConstraint.second);
        jobSpec.addRoot(opDesc);

        return jobSpec;
    }

    private static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getSplitsAndConstraints(
            ICcApplicationContext appCtx) {
        FileSplit[] splits = getSplits(appCtx.getClusterStateManager());
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    private static FileSplit[] getSplits(IClusterStateManager clusterStateManager) {
        ClusterPartition[] clusterPartitions = clusterStateManager.getClusterPartitons();
        Arrays.sort(clusterPartitions,
                Comparator.comparing(ClusterPartition::getNodeId).thenComparingInt(ClusterPartition::getIODeviceNum));
        List<FileSplit> splits = new ArrayList<>();
        for (ClusterPartition partition : clusterPartitions) {
            String activeNodeId = partition.getActiveNodeId();
            int n = splits.size();
            if (n > 0 && splits.get(n - 1).getNodeName().equals(activeNodeId)) {
                continue;
            }
            FileSplit split = StoragePathUtil.getFileSplitForClusterPartition(partition, ".");
            splits.add(split);
        }
        return splits.toArray(new FileSplit[0]);
    }
}