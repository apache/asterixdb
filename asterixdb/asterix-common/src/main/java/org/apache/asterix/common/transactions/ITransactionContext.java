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
package org.apache.asterix.common.transactions;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

/**
 * A typical transaction lifecycle goes through the following steps:
 * 1. {@link ITransactionContext#register(long, int, ILSMIndex, IModificationOperationCallback, boolean)}
 * 2. {@link ITransactionContext#beforeOperation(long)}
 * 4. {@link ITransactionContext#notifyEntityCommitted}
 * 5. {@link ITransactionContext#afterOperation(long)}
 * 6. {@link ITransactionContext#complete()}
 */
public interface ITransactionContext {

    /**
     * Registers {@link ILSMIndex} in the transaction. Registering an index
     * must be done before any operation is performed on the index by this
     * transaction.
     *
     * @param resourceId
     * @param partition
     * @param index
     * @param callback
     * @param primaryIndex
     */
    void register(long resourceId, int partition, ILSMIndex index, IModificationOperationCallback callback,
            boolean primaryIndex);

    /**
     * Gets the unique transaction id.
     *
     * @return the unique transaction id
     */
    TxnId getTxnId();

    /**
     * Sets a flag indicating that the transaction timed out.
     *
     * @param isTimeout
     */
    void setTimeout(boolean isTimeout);

    /**
     * Tests if the transaction was timed out.
     *
     * @return true if this transaction timed out. Otherwise false.
     */
    boolean isTimeout();

    /**
     * Sets the state if this transaction.
     *
     * @param txnState
     */
    void setTxnState(int txnState);

    /**
     * Gets the current state of this transaction.
     *
     * @return the current state of this transaction
     */
    int getTxnState();

    /**
     * Gets the first log sequence number of this transaction.
     *
     * @return the first log sequence number
     */
    long getFirstLSN();

    /**
     * Gets the last log sequence number of this transactions.
     *
     * @return the last log sequence number
     */
    long getLastLSN();

    /**
     * Sets the last log sequence number of this transactions.
     *
     * @param newValue
     */
    void setLastLSN(long newValue);

    /**
     * Tests if this is a write transaction.
     *
     * @return true if this is a write transaction, otherwise false.
     */
    boolean isWriteTxn();

    /**
     * Sets a flag indication that this is a write transaction.
     *
     * @param isWriterTxn
     */
    void setWriteTxn(boolean isWriterTxn);

    /**
     * Called before an operation is performed on index
     * with resource id {@code resourceId}.
     *
     * @param resourceId
     */
    void beforeOperation(long resourceId);

    /**
     * Called to notify the transaction that an entity commit
     * log belonging to this transaction has been flushed to
     * disk.
     *
     * @param partition
     */
    void notifyEntityCommitted(int partition);

    /**
     * Called after an operation is performed on index
     * with resource id {@code resourceId}.
     *
     * @param resourceId
     */
    void afterOperation(long resourceId);

    /**
     * Called when no further operations will be performed by the transaction
     * so that any resources held by the transaction may be released
     */
    void complete();
}
