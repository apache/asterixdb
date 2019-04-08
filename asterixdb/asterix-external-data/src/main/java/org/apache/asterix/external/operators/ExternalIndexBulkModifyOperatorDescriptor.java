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
package org.apache.asterix.external.operators;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;

public class ExternalIndexBulkModifyOperatorDescriptor extends TreeIndexBulkLoadOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] deletedFiles;
    private final ITupleFilterFactory tupleFilterFactory;

    public ExternalIndexBulkModifyOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory dataflowHelperFactory, int[] deletedFiles, int[] fieldPermutation,
            float fillFactor, boolean verifyInput, long numElementsHint, ITupleFilterFactory tupleFilterFactory) {
        super(spec, null, fieldPermutation, fillFactor, verifyInput, numElementsHint, false, dataflowHelperFactory,
                tupleFilterFactory);
        this.deletedFiles = deletedFiles;
        this.tupleFilterFactory = tupleFilterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new ExternalIndexBulkModifyOperatorNodePushable(indexHelperFactory, ctx, partition, fieldPermutation,
                fillFactor, verifyInput, numElementsHint,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), deletedFiles, tupleFilterFactory);
    }

}
