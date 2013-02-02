/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * Provides API for failure recovery. Failure could be at application level and
 * require a roll back or could be at system level (crash) and require a more
 * sophisticated mechanism of replaying logs and bringing the system to a
 * consistent state ensuring durability.
 */
public interface IRecoveryManager {

    public enum SystemState {
        RECOVERING,
        HEALTHY,
        CORRUPTED
    }

    /**
     * Returns the state of the system.
     * Health state of the system could be any one of the following. RECOVERING:
     * The system is recovering HEALTHY: The system is in healthy state
     * CORRUPTEED: The system is in corrupted state. This happens when a
     * rollback or recovery task fails. In this state the system is unusable.
     * 
     * @see SystemState
     * @return SystemState The state of the system
     * @throws ACIDException
     */
    SystemState getSystemState() throws ACIDException;

    /**
     * Initiates a crash recovery.
     * 
     * @param synchronous
     *            indicates if the recovery is to be done in a synchronous
     *            manner. In asynchronous mode, the recovery will happen as part
     *            of a separate thread.
     * @return SystemState the state of the system (@see SystemState) post
     *         recovery.
     * @throws ACIDException
     */
    public void startRecovery(boolean synchronous) throws IOException, ACIDException;

    /**
     * Rolls back a transaction.
     * 
     * @param txnContext
     *            the transaction context associated with the transaction
     * @throws ACIDException
     */
    public void rollbackTransaction(TransactionContext txnContext) throws ACIDException;

    public void checkpoint() throws ACIDException;
}
