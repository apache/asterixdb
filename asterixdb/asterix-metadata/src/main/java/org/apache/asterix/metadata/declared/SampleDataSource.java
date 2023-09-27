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
package org.apache.asterix.metadata.declared;

import java.util.List;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.impls.DefaultTupleProjectorFactory;

public class SampleDataSource extends DataSource {

    private final Dataset dataset;

    private final String sampleIndexName;

    public SampleDataSource(Dataset dataset, String sampleIndexName, IAType itemType, IAType metaItemType,
            INodeDomain domain) throws AlgebricksException {
        super(createSampleDataSourceId(dataset, sampleIndexName), itemType, metaItemType, Type.SAMPLE, domain);
        this.dataset = dataset;
        this.sampleIndexName = sampleIndexName;
        this.schemaTypes = DatasetDataSource.createSchemaTypesForInternalDataset(itemType, metaItemType,
                (InternalDatasetDetails) dataset.getDatasetDetails());
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionInfo) throws AlgebricksException {
        return metadataProvider.getBtreeSearchRuntime(jobSpec, opSchema, typeEnv, context, true, false, null, dataset,
                sampleIndexName, null, null, true, true, false, null, null, null, tupleFilterFactory, outputLimit,
                false, false, DefaultTupleProjectorFactory.INSTANCE, false);
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return false;
    }

    public Dataset getDataset() {
        return dataset;
    }

    private static DataSourceId createSampleDataSourceId(Dataset dataset, String sampleIndexName) {
        return new DataSourceId(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(),
                new String[] { sampleIndexName });
    }
}
