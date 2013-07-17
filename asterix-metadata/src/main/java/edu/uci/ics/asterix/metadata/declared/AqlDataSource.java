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

package edu.uci.ics.asterix.metadata.declared;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;

public class AqlDataSource implements IDataSource<AqlSourceId> {

    private AqlSourceId id;
    private Dataset dataset;
    private IAType[] schemaTypes;
    private INodeDomain domain;
    private AqlDataSourceType datasourceType;

    public enum AqlDataSourceType {
        INTERNAL,
        FEED,
        EXTERNAL,
        EXTERNAL_FEED
    }

    public AqlDataSource(AqlSourceId id, Dataset dataset, IAType itemType, AqlDataSourceType datasourceType)
            throws AlgebricksException {
        this.id = id;
        this.dataset = dataset;
        this.datasourceType = datasourceType;
        try {
            switch (datasourceType) {
                case FEED:
                    initFeedDataset(itemType, dataset);
                case INTERNAL: {
                    initInternalDataset(itemType);
                    break;
                }
                case EXTERNAL_FEED:
                case EXTERNAL: {
                    initExternalDataset(itemType);
                    break;
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public AqlDataSource(AqlSourceId id, Dataset dataset, IAType itemType) throws AlgebricksException {
        this.id = id;
        this.dataset = dataset;
        try {
            switch (dataset.getDatasetType()) {
                case FEED:
                    initFeedDataset(itemType, dataset);
                    break;
                case INTERNAL:
                    initInternalDataset(itemType);
                    break;
                case EXTERNAL: {
                    initExternalDataset(itemType);
                    break;
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    // TODO: Seems like initFeedDataset() could simply call this method.
    private void initInternalDataset(IAType itemType) throws IOException {
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        ARecordType recordType = (ARecordType) itemType;
        int n = partitioningKeys.size();
        schemaTypes = new IAType[n + 1];
        for (int i = 0; i < n; i++) {
            schemaTypes[i] = recordType.getFieldType(partitioningKeys.get(i));
        }
        schemaTypes[n] = itemType;
        domain = new DefaultNodeGroupDomain(DatasetUtils.getNodegroupName(dataset));
    }

    private void initFeedDataset(IAType itemType, Dataset dataset) throws IOException {
        if (dataset.getDatasetDetails() instanceof ExternalDatasetDetails) {
            initExternalDataset(itemType);
        } else {
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            int n = partitioningKeys.size();
            schemaTypes = new IAType[n + 1];
            ARecordType recordType = (ARecordType) itemType;
            for (int i = 0; i < n; i++) {
                schemaTypes[i] = recordType.getFieldType(partitioningKeys.get(i));
            }
            schemaTypes[n] = itemType;
            domain = new DefaultNodeGroupDomain(DatasetUtils.getNodegroupName(dataset));
        }
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        INodeDomain domainForExternalData = new INodeDomain() {
            @Override
            public Integer cardinality() {
                return null;
            }

            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }
        };
        domain = domainForExternalData;
    }

    @Override
    public AqlSourceId getId() {
        return id;
    }

    public Dataset getDataset() {
        return dataset;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public String toString() {
        return id.toString();
        // return "AqlDataSource(\"" + id.getDataverseName() + "/" +
        // id.getDatasetName() + "\")";
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new AqlDataSourcePartitioningProvider(dataset.getDatasetType(), domain);
    }

    @Override
    public void computeFDs(List<LogicalVariable> scanVariables, List<FunctionalDependency> fdList) {
        int n = scanVariables.size();
        if (n > 1) {
            List<LogicalVariable> head = new ArrayList<LogicalVariable>(scanVariables.subList(0, n - 1));
            List<LogicalVariable> tail = new ArrayList<LogicalVariable>(1);
            tail.addAll(scanVariables);
            FunctionalDependency fd = new FunctionalDependency(head, tail);
            fdList.add(fd);
        }
    }

    private static class AqlDataSourcePartitioningProvider implements IDataSourcePropertiesProvider {

        private INodeDomain domain;

        private DatasetType datasetType;

        public AqlDataSourcePartitioningProvider(DatasetType datasetType, INodeDomain domain) {
            this.datasetType = datasetType;
            this.domain = domain;
        }

        @Override
        public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
            switch (datasetType) {
                case EXTERNAL: {
                    IPartitioningProperty pp = new RandomPartitioningProperty(domain);
                    List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
                    return new StructuralPropertiesVector(pp, propsLocal);
                }
                case FEED: {
                    int n = scanVariables.size();
                    IPartitioningProperty pp;
                    if (n < 2) {
                        pp = new RandomPartitioningProperty(domain);
                    } else {
                        Set<LogicalVariable> pvars = new ListSet<LogicalVariable>();
                        int i = 0;
                        for (LogicalVariable v : scanVariables) {
                            pvars.add(v);
                            ++i;
                            if (i >= n - 1) {
                                break;
                            }
                        }
                        pp = new UnorderedPartitionedProperty(pvars, domain);
                    }
                    List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
                    return new StructuralPropertiesVector(pp, propsLocal);
                }
                case INTERNAL: {
                    int n = scanVariables.size();
                    IPartitioningProperty pp;
                    if (n < 2) {
                        pp = new RandomPartitioningProperty(domain);
                    } else {
                        Set<LogicalVariable> pvars = new ListSet<LogicalVariable>();
                        int i = 0;
                        for (LogicalVariable v : scanVariables) {
                            pvars.add(v);
                            ++i;
                            if (i >= n - 1) {
                                break;
                            }
                        }
                        pp = new UnorderedPartitionedProperty(pvars, domain);
                    }
                    List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
                    for (int i = 0; i < n - 1; i++) {
                        propsLocal.add(new LocalOrderProperty(new OrderColumn(scanVariables.get(i), OrderKind.ASC)));
                    }
                    return new StructuralPropertiesVector(pp, propsLocal);
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        }

    }

    public AqlDataSourceType getDatasourceType() {
        return datasourceType;
    }

}
