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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;

public class DataSourcePartitioningProvider implements IDataSourcePropertiesProvider {

    private final DataSource ds;
    private final INodeDomain domain;

    public DataSourcePartitioningProvider(DataSource dataSource, INodeDomain domain) {
        this.ds = dataSource;
        this.domain = domain;
    }

    @Override
    public IPhysicalPropertiesVector computeRequiredProperties(List<LogicalVariable> scanVariables,
            IOptimizationContext ctx) throws AlgebricksException {
        IPhysicalPropertiesVector propsVector;
        IPartitioningProperty pp;
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        switch (ds.getDatasourceType()) {
            case DataSource.Type.LOADABLE:
            case DataSource.Type.EXTERNAL_DATASET:
                pp = new RandomPartitioningProperty(domain);
                ds.computeLocalStructuralProperties(propsLocal, scanVariables);
                break;
            case DataSource.Type.FEED:
                String dsName = ((FeedDataSource) ds).getTargetDataset();
                Dataset feedDs = ((MetadataProvider) ctx.getMetadataProvider())
                        .findDataset(ds.getId().getDatabaseName(), ds.getId().getDataverseName(), dsName);
                PartitioningProperties partitioningProperties =
                        ((MetadataProvider) ctx.getMetadataProvider()).getPartitioningProperties(feedDs);
                pp = getFeedDatasetPartitioningProperty(ds, domain, scanVariables,
                        partitioningProperties.getComputeStorageMap());
                break;
            case DataSource.Type.INTERNAL_DATASET:
            case DataSource.Type.SAMPLE:
                Set<LogicalVariable> pvars = new ListSet<>();
                Dataset dataset;
                if (ds.getDatasourceType() == DataSource.Type.INTERNAL_DATASET) {
                    dataset = ((DatasetDataSource) ds).getDataset();
                } else {
                    dataset = ((SampleDataSource) ds).getDataset();
                }
                int[][] computeStorageMap = ((MetadataProvider) ctx.getMetadataProvider())
                        .getPartitioningProperties(dataset).getComputeStorageMap();
                pp = getInternalDatasetPartitioningProperty(ds, domain, scanVariables, pvars, computeStorageMap);
                propsLocal.add(new LocalOrderProperty(getOrderColumns(pvars)));
                break;
            default:
                throw new IllegalArgumentException();
        }
        propsVector = new StructuralPropertiesVector(pp, propsLocal);
        return propsVector;
    }

    @Override
    public IPhysicalPropertiesVector computeDeliveredProperties(List<LogicalVariable> scanVariables,
            IOptimizationContext ctx) throws AlgebricksException {
        switch (ds.getDatasourceType()) {
            case DataSource.Type.INTERNAL_DATASET: {
                Set<LogicalVariable> pvars = new ListSet<>();
                Dataset dataset = ((DatasetDataSource) ds).getDataset();
                int[][] computeStorageMap = ((MetadataProvider) ctx.getMetadataProvider())
                        .getPartitioningProperties(dataset).getComputeStorageMap();
                IPartitioningProperty pp =
                        getInternalDatasetPartitioningProperty(ds, domain, scanVariables, pvars, computeStorageMap);
                List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
                ds.computeLocalStructuralProperties(propsLocal, scanVariables);
                return new StructuralPropertiesVector(pp, propsLocal);
            }
            case DataSource.Type.FEED: {
                IPartitioningProperty pp = getFeedPartitioningProperty(ds, domain, scanVariables);
                return new StructuralPropertiesVector(pp, new ArrayList<>());
            }
            default:
                return computeRequiredProperties(scanVariables, ctx);
        }
    }

    private static List<OrderColumn> getOrderColumns(Set<LogicalVariable> pvars) {
        List<OrderColumn> orderColumns = new ArrayList<>();
        for (LogicalVariable pkVar : pvars) {
            orderColumns.add(new OrderColumn(pkVar, OrderKind.ASC));
        }
        return orderColumns;
    }

    private static IPartitioningProperty getInternalDatasetPartitioningProperty(DataSource ds, INodeDomain domain,
            List<LogicalVariable> scanVariables, Set<LogicalVariable> pvars, int[][] partitionsMap) {
        IPartitioningProperty pp;
        if (scanVariables.size() < 2) {
            pp = new RandomPartitioningProperty(domain);
        } else {
            pvars.addAll(ds.getPrimaryKeyVariables(scanVariables));
            pp = UnorderedPartitionedProperty.ofPartitionsMap(pvars, domain, partitionsMap);
        }
        return pp;
    }

    public static IPartitioningProperty getFeedDatasetPartitioningProperty(DataSource ds, INodeDomain domain,
            List<LogicalVariable> scanVariables, int[][] partitionsMap) {
        IPartitioningProperty pp;
        if (scanVariables.size() < 2) {
            pp = new RandomPartitioningProperty(domain);
        } else {
            Set<LogicalVariable> pvars = new ListSet<>();
            pvars.addAll(ds.getPrimaryKeyVariables(scanVariables));
            pp = UnorderedPartitionedProperty.ofPartitionsMap(pvars, domain, partitionsMap);
        }
        return pp;
    }

    public static IPartitioningProperty getFeedPartitioningProperty(DataSource ds, INodeDomain domain,
            List<LogicalVariable> scanVariables) {
        IPartitioningProperty pp;
        if (scanVariables.size() < 2) {
            pp = new RandomPartitioningProperty(domain);
        } else {
            Set<LogicalVariable> pvars = new ListSet<>();
            pvars.addAll(ds.getPrimaryKeyVariables(scanVariables));
            pp = UnorderedPartitionedProperty.of(pvars, domain);
        }
        return pp;
    }

}
