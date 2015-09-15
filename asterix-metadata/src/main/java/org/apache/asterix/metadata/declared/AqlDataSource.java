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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;

public abstract class AqlDataSource implements IDataSource<AqlSourceId> {

    private final AqlSourceId id;
   private final IAType itemType;
    private final AqlDataSourceType datasourceType;
    protected IAType[] schemaTypes;
    protected INodeDomain domain;
    private Map<String, Serializable> properties = new HashMap<String, Serializable>();

    public enum AqlDataSourceType {
        INTERNAL_DATASET,
        EXTERNAL_DATASET,
        FEED,
        LOADABLE
    }

    public AqlDataSource(AqlSourceId id, String datasourceDataverse, String datasourceName,
            IAType itemType, AqlDataSourceType datasourceType) throws AlgebricksException {
        this.id = id;
        this.itemType = itemType;
        this.datasourceType = datasourceType;
    }

    public String getDatasourceDataverse() {
        return id.getDataverseName();
    }

    public String getDatasourceName() {
        return id.getDatasourceName();
    }

    @Override
    public abstract IAType[] getSchemaTypes();

    public abstract INodeDomain getDomain();

    public void computeLocalStructuralProperties(List<ILocalStructuralProperty> localProps,
            List<LogicalVariable> variables) {
        // do nothing
    }

    @Override
    public AqlSourceId getId() {
        return id;
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new AqlDataSourcePartitioningProvider(this, domain);
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

        private final AqlDataSource ds;

        private final INodeDomain domain;

        public AqlDataSourcePartitioningProvider(AqlDataSource dataSource, INodeDomain domain) {
            this.ds = dataSource;
            this.domain = domain;
        }

        @Override
        public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
            IPhysicalPropertiesVector propsVector = null;
            IPartitioningProperty pp;
            List<ILocalStructuralProperty> propsLocal;
            int n;
            switch (ds.getDatasourceType()) {
                case LOADABLE:
                case EXTERNAL_DATASET:
                    pp = new RandomPartitioningProperty(domain);
                    propsLocal = new ArrayList<ILocalStructuralProperty>();
                    ds.computeLocalStructuralProperties(propsLocal, scanVariables);
                    propsVector = new StructuralPropertiesVector(pp, propsLocal);
                    break;

                case FEED:
                    n = scanVariables.size();
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
                    propsLocal = new ArrayList<ILocalStructuralProperty>();
                    propsVector = new StructuralPropertiesVector(pp, propsLocal);
                    break;

                case INTERNAL_DATASET:
                    n = scanVariables.size();
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
                    propsLocal = new ArrayList<ILocalStructuralProperty>();
                    List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
                    for (int i = 0; i < n - 1; i++) {
                        orderColumns.add(new OrderColumn(scanVariables.get(i), OrderKind.ASC));
                    }
                    propsLocal.add(new LocalOrderProperty(orderColumns));
                    propsVector = new StructuralPropertiesVector(pp, propsLocal);
                    break;

                default:
                    throw new IllegalArgumentException();
            }
            return propsVector;
        }

    }

    public AqlDataSourceType getDatasourceType() {
        return datasourceType;
    }

    public Map<String, Serializable> getProperties() {
        return properties;
    }
    
    public IAType getItemType() {
        return itemType;
    }
    public void setProperties(Map<String, Serializable> properties) {
        this.properties = properties;
    }

}
