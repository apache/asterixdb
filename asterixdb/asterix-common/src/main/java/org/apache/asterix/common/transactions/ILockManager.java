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
 * Interface for the lockManager
 *
 * @author pouria
 * @author kisskys
 */
public interface ILockManager {

    /**
     * The method to request a specific lock mode on a specific resource by a
     * specific transaction
     * - The requesting transaction would succeed to grab the lock if its
     * request does not have any conflict with the currently held locks on the
     * resource AND if no other transaction is waiting "for conversion".
     * Otherwise the requesting transaction will be sent to wait.
     * - If the transaction already has the "same" lock, then a redundant lock
     * call would be called on the resource - If the transaction already has a
     * "stronger" lock mode, then no action would be taken - If the transaction
     * has a "weaker" lock, then the request would be interpreted as a convert
     * request
     * Waiting transaction would eventually garb the lock, or get timed-out
     *
     * @param datasetId
     * @param entityHashValue
     * @param lockMode
     * @param txnContext
     * @throws ACIDException
     */
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException;

    /**
     * The method releases "All" the locks taken/waiting-on by a specific
     * transaction on "All" resources Upon releasing each lock on each resource,
     * potential waiters, which can be waken up based on their requested lock
     * mode and the waiting policy would be waken up
     *
     * @param txnContext
     * @throws ACIDException
     */
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException;

    /**
     * @param datasetId
     * @param entityHashValue
     * @param lockMode
     * @param txnContext
     * @throws ACIDException
     * @return
     */
    public void unlock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException;

    /**
     * Call to lock and unlock a specific resource in a specific lock mode
     *
     * @param datasetId
     * @param entityHashValue
     * @param lockMode
     * @param context
     * @return
     * @throws ACIDException
     */
    public void instantLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext context)
            throws ACIDException;

    /**
     * @param datasetId
     * @param entityHashValue
     * @param lockMode
     * @param context
     * @return
     * @throws ACIDException
     */
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext context)
            throws ACIDException;

    /**
     * @param datasetId
     * @param entityHashValue
     * @param lockMode
     * @param txnContext
     * @return
     * @throws ACIDException
     */
    boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException;

    /**
     * Prints out the contents of the transactions' table in a readable fashion
     *
     * @return
     * @throws ACIDException
     */
    public String prettyPrint() throws ACIDException;

}
