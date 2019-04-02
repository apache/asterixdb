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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexBulkLoadOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;

public class ExternalIndexBulkLoadOperatorNodePushable extends IndexBulkLoadOperatorNodePushable {

    private final int version;

    public ExternalIndexBulkLoadOperatorNodePushable(IIndexDataflowHelperFactory indexDataflowHelperFactory,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex, RecordDescriptor recDesc, int version,
            ITupleFilterFactory tupleFilterFactory) throws HyracksDataException {
        super(indexDataflowHelperFactory, ctx, partition, fieldPermutation, fillFactor, verifyInput, numElementsHint,
                checkIfEmptyIndex, recDesc, tupleFilterFactory);
        this.version = version;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        ((ITwoPCIndex) index).setCurrentVersion(version);
    }

    @Override
    protected void initializeBulkLoader() throws HyracksDataException {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID, LSMComponentId.DEFAULT_COMPONENT_ID);
        bulkLoader = ((ILSMIndex) index).createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                parameters);
    }
}
