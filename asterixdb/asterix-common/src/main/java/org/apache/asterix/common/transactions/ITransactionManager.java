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

import org.apache.asterix.common.exceptions.ACIDException;

/**
 * Provides APIs for managing life cycle of a transaction, that is beginning a
 * transaction and aborting/committing the transaction.
 */
public interface ITransactionManager {

    /**
     * A transaction may be in any of the following states ACTIVE: The
     * transaction is ongoing and has not yet committed/aborted. COMMITTED: The
     * transaction has committed. ABORTED: The transaction has aborted.
     * TIMED_OUT: The transaction has timed out waiting to acquire a lock.
     */
    int ACTIVE = 0;
    int COMMITTED = 1;
    int ABORTED = 2;
    int TIMED_OUT = 3;

    enum AtomicityLevel {
        /**
         * all records are committed or nothing
         */
        ATOMIC,
        /**
         * any record with entity commit log
         */
        ENTITY_LEVEL
    }

    enum TransactionMode {
        /**
         * Transaction performs only read operations
         */
        READ,
        /**
         * Transaction may perform read and write operations
         */
        READ_WRITE
    }

    /**
     * Begins a transaction identified by a transaction id and returns the
     * associated transaction context.
     *
     * @param txnId
     * @param options
     * @return The transaction context
     * @throws ACIDException
     */
    ITransactionContext beginTransaction(TxnId txnId, TransactionOptions options) throws ACIDException;

    /**
     * Returns the transaction context of an active transaction given the
     * transaction id.
     *
     * @param txnId
     * @return The transaction context
     * @throws ACIDException
     */
    ITransactionContext getTransactionContext(TxnId txnId) throws ACIDException;

    /**
     * Commit a transactions
     *
     * @param txnId
     * @throws ACIDException
     */
    void commitTransaction(TxnId txnId) throws ACIDException;

    /**
     * Aborts a transaction.
     *
     * @param txnId
     * @throws ACIDException
     */
    void abortTransaction(TxnId txnId) throws ACIDException;

    /**
     * @return The current max txn id.
     */
    long getMaxTxnId();

    /**
     * Sets the maximum txn id to the bigger value of {@code txnId} and its current value.
     *
     * @param txnId
     */
    void ensureMaxTxnId(long txnId);

}
