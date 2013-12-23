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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.context.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;

public class PrimaryIndexInstantSearchOperationCallbackFactory extends AbstractOperationCallbackFactory implements
        ISearchOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    public PrimaryIndexInstantSearchOperationCallbackFactory(JobId jobId, int datasetId, int[] entityIdFields,
            ITransactionSubsystemProvider txnSubsystemProvider, byte resourceType) {
        super(jobId, datasetId, entityIdFields, txnSubsystemProvider, resourceType);
    }

    @Override
    public ISearchOperationCallback createSearchOperationCallback(long resourceId, IHyracksTaskContext ctx)
            throws HyracksDataException {
        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        try {
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(jobId, false);
            return new PrimaryIndexInstantSearchOperationCallback(datasetId, primaryKeyFields,
                    txnSubsystem.getLockManager(), txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

}
