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
package org.apache.asterix.runtime.job.listener;

import java.util.List;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobStatus;

/**
 * This Joblet enable transactions on multiple datasets to take place in the same Hyracks Job
 * It takes a list of Transaction job ids instead of a single job Id
 */
public class MultiTransactionJobletEventListenerFactory implements IJobletEventListenerFactory {

    private static final long serialVersionUID = 1L;
    private final List<JobId> jobIds;
    private final boolean transactionalWrite;

    public MultiTransactionJobletEventListenerFactory(List<JobId> jobIds, boolean transactionalWrite) {
        this.jobIds = jobIds;
        this.transactionalWrite = transactionalWrite;
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager =
                            ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                                    .getTransactionSubsystem().getTransactionManager();
                    for (JobId jobId : jobIds) {
                        ITransactionContext txnContext = txnManager.getTransactionContext(jobId, false);
                        txnContext.setWriteTxn(transactionalWrite);
                        txnManager.completedTransaction(txnContext, new DatasetId(-1), -1,
                                !(jobStatus == JobStatus.FAILURE));
                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    for (JobId jobId : jobIds) {
                        ((IAsterixAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                                .getTransactionSubsystem().getTransactionManager().getTransactionContext(jobId, true);
                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
