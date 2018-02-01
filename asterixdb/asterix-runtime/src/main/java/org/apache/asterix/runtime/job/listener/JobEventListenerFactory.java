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

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionManager.AtomicityLevel;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobParameterByteStore;
import org.apache.hyracks.api.job.JobStatus;

public class JobEventListenerFactory implements IJobEventListenerFactory {

    private static final long serialVersionUID = 1L;

    private TxnId txnId;
    private final boolean transactionalWrite;

    //To enable new Asterix TxnId for separate deployed job spec invocations
    private static final byte[] TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter".getBytes();

    public JobEventListenerFactory(TxnId txnId, boolean transactionalWrite) {
        this.txnId = txnId;
        this.transactionalWrite = transactionalWrite;
    }

    @Override
    public TxnId getTxnId(int datasetId) {
        return txnId;
    }

    @Override
    public IJobletEventListenerFactory copyFactory() {
        return new JobEventListenerFactory(txnId, transactionalWrite);
    }

    @Override
    public void updateListenerJobParameters(JobParameterByteStore jobParameterByteStore) {
        String AsterixTransactionIdString = new String(jobParameterByteStore
                .getParameterValue(TRANSACTION_ID_PARAMETER_NAME, 0, TRANSACTION_ID_PARAMETER_NAME.length));
        if (AsterixTransactionIdString.length() > 0) {
            this.txnId = new TxnId(Integer.parseInt(AsterixTransactionIdString));
        }
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager =
                            ((INcApplicationContext) jobletContext.getServiceContext().getApplicationContext())
                                    .getTransactionSubsystem().getTransactionManager();
                    ITransactionContext txnContext = txnManager.getTransactionContext(txnId);
                    txnContext.setWriteTxn(transactionalWrite);
                    if (jobStatus != JobStatus.FAILURE) {
                        txnManager.commitTransaction(txnId);
                    } else {
                        txnManager.abortTransaction(txnId);
                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    TransactionOptions options = new TransactionOptions(AtomicityLevel.ENTITY_LEVEL);
                    ((INcApplicationContext) jobletContext.getServiceContext().getApplicationContext())
                            .getTransactionSubsystem().getTransactionManager().beginTransaction(txnId, options);
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
