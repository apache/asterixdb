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
package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.List;

import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;

public class HiveDataSource<P> implements IDataSource<P> {

    private P source;

    private Object[] schema;

    public HiveDataSource(P dataSource, Object[] sourceSchema) {
        source = dataSource;
        schema = sourceSchema;
    }

    @Override
    public P getId() {
        return source;
    }

    @Override
    public Object[] getSchemaTypes() {
        return schema;
    }

    @Override
    public void computeFDs(List<LogicalVariable> scanVariables, List<FunctionalDependency> fdList) {
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return new HiveDataSourcePartitioningProvider();
    }

    @Override
    public String toString() {
        PartitionDesc desc = (PartitionDesc) source;
        return desc.getTableName();
    }
}
