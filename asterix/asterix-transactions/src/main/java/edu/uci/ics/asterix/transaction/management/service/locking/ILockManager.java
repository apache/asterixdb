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
package edu.uci.ics.asterix.transaction.management.service.locking;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * @author pouria Interface for the lockManager
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
     * @param resourceID
     * @param mode
     * @return
     * @throws ACIDException
     */
    public boolean lock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException;

    /**
     * The method releases "All" the locks taken/waiting-on by a specific
     * transaction on "All" resources Upon releasing each lock on each resource,
     * potential waiters, which can be waken up based on their requested lock
     * mode and the waiting policy would be waken up
     * 
     * @param context
     * @return
     * @throws ACIDException
     */
    public Boolean releaseLocks(TransactionContext context) throws ACIDException;

    /**
     * Releases "All" the locks by a transaction on a "single specific" resource
     * Upon releasing, potential waiters, which can be waken up based on their
     * requested lock mode and the waiting policy would be waken up
     * 
     * @param resourceID
     * @return
     * @throws ACIDException
     */
    public boolean unlock(TransactionContext context, byte[] resourceID) throws ACIDException;

    /**
     * Request to convert granted lockMode of a transaction on a specific
     * resource. Requesting transaction would either grab the lock, or sent to
     * waiting based on the type of the request, and current mask on the
     * resource and possible set of waiting converters
     * - If the transaction does not have any lock on the resource, then an
     * exception is thrown - If the transaction already has a stronger lock,
     * then no operation is taken
     * 
     * @param context
     * @param resourceID
     * @param mode
     * @return
     * @throws ACIDException
     */
    public boolean convertLock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException;

    /**
     * Call to lock and unlock a specific resource in a specific lock mode
     * 
     * @param context
     * @param resourceID
     * @param mode
     * @param timeout
     * @return
     * @throws ACIDException
     */
    public boolean getInstantlock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException;

    /**
     * Prints out the contents of the transactions' table in a readable fashion
     * 
     * @return
     * @throws ACIDException
     */
    public String getDebugLockStatus() throws ACIDException;

}
