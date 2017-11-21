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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.impl.AbstractReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public class LSMIndexReplicationJob extends AbstractReplicationJob implements ILSMIndexReplicationJob {

    private final ILSMIndex lsmIndex;
    private final ILSMIndexOperationContext operationContext;
    private final LSMOperationType LSMOpType;

    public LSMIndexReplicationJob(ILSMIndex lsmIndex, ILSMIndexOperationContext operationContext,
            Set<String> filesToReplicate, ReplicationOperation operation, ReplicationExecutionType executionType,
            LSMOperationType opType) {
        super(ReplicationJobType.LSM_COMPONENT, operation, executionType, filesToReplicate);
        this.lsmIndex = lsmIndex;
        this.operationContext = operationContext;
        this.LSMOpType = opType;
    }

    @Override
    public void endReplication() throws HyracksDataException {
        if (operationContext != null) {
            ((AbstractLSMIndex) (lsmIndex)).getHarness().endReplication(operationContext);
        }
    }

    @Override
    public ILSMIndex getLSMIndex() {
        return lsmIndex;
    }

    @Override
    public ILSMIndexOperationContext getLSMIndexOperationContext() {
        return operationContext;
    }

    @Override
    public LSMOperationType getLSMOpType() {
        return LSMOpType;
    }
}
