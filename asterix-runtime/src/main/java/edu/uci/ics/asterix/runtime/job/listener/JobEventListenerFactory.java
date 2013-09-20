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
package edu.uci.ics.asterix.runtime.job.listener;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.JobStatus;

public class JobEventListenerFactory implements IJobletEventListenerFactory {

    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final boolean transactionalWrite;

    public JobEventListenerFactory(JobId jobId, boolean transactionalWrite) {
        this.jobId = jobId;
        this.transactionalWrite = transactionalWrite;
    }

    public JobId getJobId() {
        return jobId;
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager = ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext()
                            .getApplicationObject()).getTransactionSubsystem().getTransactionManager();
                    ITransactionContext txnContext = txnManager.getTransactionContext(jobId, false);
                    txnContext.setWriteTxn(transactionalWrite);
                    txnManager.completedTransaction(txnContext, new DatasetId(-1), -1,
                            !(jobStatus == JobStatus.FAILURE));
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                            .getTransactionSubsystem().getTransactionManager().getTransactionContext(jobId, true);
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
