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

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class DumpIndexFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 1L;
    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final RecordDescriptor recDesc;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public DumpIndexFunction(AlgebricksAbsolutePartitionConstraint locations,
            IndexDataflowHelperFactory indexDataflowHelperFactory, RecordDescriptor recDesc,
            IBinaryComparatorFactory[] comparatorFactories) {
        super(locations);
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.recDesc = recDesc;
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        final IIndexDataflowHelper indexDataflowHelper = indexDataflowHelperFactory.create(serviceCtx, partition);
        return new DumpIndexReader(indexDataflowHelper, recDesc, comparatorFactories);
    }
}
