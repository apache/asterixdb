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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.impl.AbstractReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;

public class LSMIndexReplicationJob extends AbstractReplicationJob implements ILSMIndexReplicationJob {

    private final AbstractLSMIndex lsmIndex;
    private final ILSMIndexOperationContext ctx;

    public LSMIndexReplicationJob(AbstractLSMIndex lsmIndex, ILSMIndexOperationContext ctx,
            Set<String> filesToReplicate, ReplicationOperation operation, ReplicationExecutionType executionType) {
        super(ReplicationJobType.LSM_COMPONENT, operation, executionType, filesToReplicate);
        this.lsmIndex = lsmIndex;
        this.ctx = ctx;
    }

    @Override
    public void endReplication() throws HyracksDataException {
        if (ctx != null) {
            lsmIndex.lsmHarness.endReplication(ctx);
        }
    }
}
