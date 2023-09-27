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
package org.apache.asterix.app.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.impls.DefaultTupleProjectorFactory;

public class QueryIndexDatasource extends FunctionDataSource {

    private final Dataset ds;
    private final String indexName;
    private final AlgebricksAbsolutePartitionConstraint storageLocations;
    private final int numSecKeys;

    public QueryIndexDatasource(Dataset ds, String indexName, INodeDomain domain,
            AlgebricksAbsolutePartitionConstraint storageLocations, ARecordType recType, int numSecKeys)
            throws AlgebricksException {
        super(createQueryIndexDataSourceId(ds, indexName), QueryIndexRewriter.QUERY_INDEX, domain, recType);
        this.ds = ds;
        this.indexName = indexName;
        this.storageLocations = storageLocations;
        this.numSecKeys = numSecKeys;
    }

    @Override
    protected void initSchemaType(IAType iType) {
        ARecordType type = (ARecordType) iType;
        IAType[] fieldTypes = type.getFieldTypes();
        schemaTypes = new IAType[fieldTypes.length];
        System.arraycopy(fieldTypes, 0, schemaTypes, 0, schemaTypes.length);
    }

    @Override
    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm) {
        return storageLocations;
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        // the index scan op is not a leaf op. the ETS op will start the scan of the index. we need the ETS op below
        // the index scan to be still generated
        return false;
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        throw new UnsupportedOperationException("query-index() does not use record reader adapter");
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionInfo) throws AlgebricksException {
        return metadataProvider.getBtreeSearchRuntime(jobSpec, opSchema, typeEnv, context, true, false, null, ds,
                indexName, null, null, true, true, false, null, null, null, tupleFilterFactory, outputLimit, false,
                false, DefaultTupleProjectorFactory.INSTANCE, false);
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computeRequiredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                return StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
            }

            @Override
            public IPhysicalPropertiesVector computeDeliveredProperties(List<LogicalVariable> scanVariables,
                    IOptimizationContext ctx) {
                List<ILocalStructuralProperty> propsLocal = new ArrayList<>(1);
                //TODO(ali): consider primary keys?
                List<OrderColumn> secKeys = new ArrayList<>(numSecKeys);
                for (int i = 0; i < numSecKeys; i++) {
                    secKeys.add(new OrderColumn(scanVariables.get(i), OrderOperator.IOrder.OrderKind.ASC));
                }
                propsLocal.add(new LocalOrderProperty(secKeys));
                return new StructuralPropertiesVector(new RandomPartitioningProperty(domain), propsLocal);
            }
        };
    }

    private static DataSourceId createQueryIndexDataSourceId(Dataset dataset, String indexName) {
        return new DataSourceId(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(),
                new String[] { indexName, QueryIndexRewriter.QUERY_INDEX.getName() });
    }
}
