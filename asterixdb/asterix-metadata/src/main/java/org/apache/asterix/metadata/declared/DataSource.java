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

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public abstract class DataSource implements IDataSource<DataSourceId> {

    protected final DataSourceId id;
    protected final IAType itemType;
    protected final IAType metaItemType;
    protected final byte datasourceType;
    protected IAType[] schemaTypes;
    protected INodeDomain domain;
    protected Map<String, Serializable> properties = new HashMap<>();

    public static class Type {
        // positive range is reserved for core datasource types
        public static final byte INTERNAL_DATASET = 0x00;
        public static final byte EXTERNAL_DATASET = 0x01;
        public static final byte FEED = 0x02;
        public static final byte LOADABLE = 0x03;
        public static final byte FUNCTION = 0x04;

        // Hide implicit public constructor
        private Type() {
        }
    }

    public DataSource(DataSourceId id, IAType itemType, IAType metaItemType, byte datasourceType, INodeDomain domain)
            throws AlgebricksException {
        this.id = id;
        this.itemType = itemType;
        this.metaItemType = metaItemType;
        this.datasourceType = datasourceType;
        this.domain = domain;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public INodeDomain getDomain() {
        return domain;
    }

    public void computeLocalStructuralProperties(List<ILocalStructuralProperty> localProps,
            List<LogicalVariable> variables) {
        // do nothing
    }

    @Override
    public DataSourceId getId() {
        return id;
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new DataSourcePartitioningProvider(this, domain);
    }

    @Override
    public void computeFDs(List<LogicalVariable> scanVariables, List<FunctionalDependency> fdList) {
        int n = scanVariables.size();
        if (n > 1) {
            List<LogicalVariable> head = new ArrayList<>(scanVariables.subList(0, n - 1));
            List<LogicalVariable> tail = new ArrayList<>(1);
            tail.addAll(scanVariables);
            FunctionalDependency fd = new FunctionalDependency(head, tail);
            fdList.add(fd);
        }
    }

    public byte getDatasourceType() {
        return datasourceType;
    }

    public Map<String, Serializable> getProperties() {
        return properties;
    }

    public IAType getItemType() {
        return itemType;
    }

    public IAType getMetaItemType() {
        return metaItemType;
    }

    public boolean hasMeta() {
        return metaItemType != null;
    }

    public void setProperties(Map<String, Serializable> properties) {
        this.properties = properties;
    }

    public LogicalVariable getMetaVariable(List<LogicalVariable> dataScanVariables) {
        if (hasMeta()) {
            return dataScanVariables.get(dataScanVariables.size() - 1);
        } else {
            return null;
        }
    }

    public LogicalVariable getDataRecordVariable(List<LogicalVariable> dataScanVariables) {
        return hasMeta() ? dataScanVariables.get(dataScanVariables.size() - 2)
                : dataScanVariables.get(dataScanVariables.size() - 1);
    }

    public List<LogicalVariable> getPrimaryKeyVariables(List<LogicalVariable> dataScanVariables) {
        return new ArrayList<>(dataScanVariables.subList(0, dataScanVariables.size() - (hasMeta() ? 2 : 1)));
    }

    public abstract Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException;
}
