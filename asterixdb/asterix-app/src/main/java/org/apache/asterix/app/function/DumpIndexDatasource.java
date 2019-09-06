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

import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class DumpIndexDatasource extends FunctionDataSource {

    public static final DataSourceId DUMP_INDEX_DATASOURCE_ID = createDataSourceId(DumpIndexRewriter.DUMP_INDEX);

    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final RecordDescriptor recDesc;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public DumpIndexDatasource(INodeDomain domain, IndexDataflowHelperFactory indexDataflowHelperFactory,
            RecordDescriptor recDesc, IBinaryComparatorFactory[] comparatorFactories) throws AlgebricksException {
        super(DUMP_INDEX_DATASOURCE_ID, domain);
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.recDesc = recDesc;
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        return new DumpIndexFunction(metadataProvider.getClusterLocations(), indexDataflowHelperFactory, recDesc,
                comparatorFactories);
    }
}
