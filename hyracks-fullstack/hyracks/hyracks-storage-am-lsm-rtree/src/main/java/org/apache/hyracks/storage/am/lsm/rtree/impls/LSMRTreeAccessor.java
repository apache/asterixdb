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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.DualTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;

public class LSMRTreeAccessor extends LSMTreeIndexAccessor {
    private final DualTupleReference dualTuple;

    public LSMRTreeAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx, int[] buddyBTreeFields) {
        super(lsmHarness, ctx, aCtx -> new LSMRTreeSearchCursor(aCtx, buddyBTreeFields));
        dualTuple = new DualTupleReference(buddyBTreeFields);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        getCtx().setOperation(IndexOperation.DELETE);
        dualTuple.reset(tuple);
        lsmHarness.modify(getCtx(), false, dualTuple);
    }

    @Override
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException {
        getCtx().setOperation(IndexOperation.DELETE);
        dualTuple.reset(tuple);
        return lsmHarness.modify(getCtx(), true, dualTuple);
    }

    @Override
    public void forceDelete(ITupleReference tuple) throws HyracksDataException {
        getCtx().setOperation(IndexOperation.DELETE);
        dualTuple.reset(tuple);
        lsmHarness.forceModify(getCtx(), dualTuple);
    }
}