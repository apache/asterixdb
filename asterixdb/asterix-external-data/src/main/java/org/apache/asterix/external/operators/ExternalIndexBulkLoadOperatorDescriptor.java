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
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;

public class ExternalIndexBulkLoadOperatorDescriptor extends TreeIndexBulkLoadOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int version;
    private final ITupleFilterFactory tupleFilterFactory;

    public ExternalIndexBulkLoadOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IIndexDataflowHelperFactory indexHelperFactory, int version,
            ITupleFilterFactory tupleFilterFactory) {
        super(spec, outRecDesc, fieldPermutation, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                indexHelperFactory);
        this.version = version;
        this.tupleFilterFactory = tupleFilterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new ExternalIndexBulkLoadOperatorNodePushable(indexHelperFactory, ctx, partition, fieldPermutation,
                fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0), version, tupleFilterFactory);
    }

}
