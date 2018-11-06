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

package org.apache.asterix.metadata.utils;

import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;

import java.util.Set;

import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.common.IResourceFactory;

public abstract class SecondaryTreeIndexOperationsHelper extends SecondaryIndexOperationsHelper {

    protected SecondaryTreeIndexOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IResourceFactory resourceFactory = dataset.getResourceFactory(metadataProvider, index, itemType, metaType,
                mergePolicyFactory, mergePolicyProperties);
        IIndexBuilderFactory indexBuilderFactory = new IndexBuilderFactory(storageComponentProvider.getStorageManager(),
                secondaryFileSplitProvider, resourceFactory, true);
        IndexCreateOperatorDescriptor secondaryIndexCreateOp =
                new IndexCreateOperatorDescriptor(spec, indexBuilderFactory);
        secondaryIndexCreateOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(secondaryIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildDropJobSpec(Set<DropOption> options) throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, index.getIndexName());
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), splitsAndConstraint.first);
        // The index drop operation should be persistent regardless of temp datasets or permanent dataset.
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec, dataflowHelperFactory, options);
        btreeDrop.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, btreeDrop,
                splitsAndConstraint.second);
        spec.addRoot(btreeDrop);
        return spec;
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(dataset, index.getIndexName());
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), splitsAndConstraint.first);
        LSMTreeIndexCompactOperatorDescriptor compactOp =
                new LSMTreeIndexCompactOperatorDescriptor(spec, dataflowHelperFactory);
        compactOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);
        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
