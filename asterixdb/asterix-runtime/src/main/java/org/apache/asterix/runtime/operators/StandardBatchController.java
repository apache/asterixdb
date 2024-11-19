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
package org.apache.asterix.runtime.operators;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IBatchController;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

class StandardBatchController implements IBatchController {
    static final IBatchController INSTANCE = new StandardBatchController();

    private StandardBatchController() {
    }

    @Override
    public void batchEnter(ILSMIndexOperationContext ctx, ILSMHarness lsmHarness, IFrameOperationCallback callback)
            throws HyracksDataException {
        lsmHarness.enter(ctx, LSMOperationType.MODIFICATION);
    }

    @Override
    public void batchExit(ILSMIndexOperationContext ctx, ILSMHarness lsmHarness, IFrameOperationCallback callback,
            boolean batchSuccessful) throws HyracksDataException {
        lsmHarness.exit(ctx, callback, batchSuccessful, LSMOperationType.MODIFICATION);
    }
}
