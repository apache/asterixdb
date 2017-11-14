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
package org.apache.asterix.transaction.management.service.locking;

import java.io.PrintStream;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;

/**
 * repesents a lock request for testing.
 */
abstract class Request {
    /** the kind of a request */
    enum Kind {
        /** requests an instant-try-lock */
        INSTANT_TRY_LOCK,
        /** requests an instant-lock */
        INSTANT_LOCK,
        /** requests a lock */
        LOCK,
        /** prints a JSON representation of the lock table by entity */
        PRINT,
        /** releases all locks */
        RELEASE,
        /** requests a try-lock */
        TRY_LOCK,
        /** unlocks a lock */
        UNLOCK
    }

    Kind kind;
    ITransactionContext txnCtx;

    Request(Kind kind, ITransactionContext txnCtx) {
        this.kind = kind;
        this.txnCtx = txnCtx;
    }

    String asString(final Kind kind, final ITransactionContext txnCtx, final DatasetId dsId, final int hashValue,
            final byte lockMode) {
        return txnCtx.getTxnId() + ":" + kind.name() + ":" + dsId.getId() + ":" + hashValue + ":"
                + TransactionManagementConstants.LockManagerConstants.LockMode.toString(lockMode);
    }

    abstract boolean execute(ILockManager lockMgr) throws ACIDException;

    static Request create(final Kind kind, final ITransactionContext txnCtx, final DatasetId dsId, final int hashValue,
            final byte lockMode) {

        switch (kind) {
            case INSTANT_TRY_LOCK:
                return new Request(kind, txnCtx) {
                    @Override
                    boolean execute(ILockManager lockMgr) throws ACIDException {
                        return lockMgr.instantTryLock(dsId, hashValue, lockMode, txnCtx);
                    }

                    @Override
                    public String toString() {
                        return asString(kind, txnCtx, dsId, hashValue, lockMode);
                    }
                };
            case INSTANT_LOCK:
                return new Request(kind, txnCtx) {
                    @Override
                    boolean execute(ILockManager lockMgr) throws ACIDException {
                        lockMgr.instantLock(dsId, hashValue, lockMode, txnCtx);
                        return true;
                    }

                    @Override
                    public String toString() {
                        return asString(kind, txnCtx, dsId, hashValue, lockMode);
                    }
                };
            case LOCK:
                return new Request(kind, txnCtx) {
                    @Override
                    boolean execute(ILockManager lockMgr) throws ACIDException {
                        lockMgr.lock(dsId, hashValue, lockMode, txnCtx);
                        return true;
                    }

                    @Override
                    public String toString() {
                        return asString(kind, txnCtx, dsId, hashValue, lockMode);
                    }
                };
            case TRY_LOCK:
                return new Request(kind, txnCtx) {
                    @Override
                    boolean execute(ILockManager lockMgr) throws ACIDException {
                        return lockMgr.tryLock(dsId, hashValue, lockMode, txnCtx);
                    }

                    @Override
                    public String toString() {
                        return asString(kind, txnCtx, dsId, hashValue, lockMode);
                    }
                };
            case UNLOCK:
                return new Request(kind, txnCtx) {
                    @Override
                    boolean execute(ILockManager lockMgr) throws ACIDException {
                        lockMgr.unlock(dsId, hashValue, lockMode, txnCtx);
                        return true;
                    }

                    @Override
                    public String toString() {
                        return asString(kind, txnCtx, dsId, hashValue, lockMode);
                    }
                };
            default:
        }
        throw new AssertionError("Illegal Request Kind " + kind);
    }

    static Request create(final Kind kind, final ITransactionContext txnCtx) {
        if (kind == Kind.RELEASE) {
            return new Request(kind, txnCtx) {
                @Override
                boolean execute(ILockManager lockMgr) throws ACIDException {
                    lockMgr.releaseLocks(txnCtx);
                    return true;
                }

                @Override
                public String toString() {
                    return txnCtx.getTxnId().toString() + ":" + kind.name();
                }
            };
        }
        throw new AssertionError("Illegal Request Kind " + kind);
    }

    static Request create(final Kind kind, final PrintStream out) {
        if (kind == Kind.PRINT) {
            return new Request(kind, null) {
                @Override
                boolean execute(ILockManager lockMgr) throws ACIDException {
                    if (out == null) {
                        return false;
                    }
                    if (!(lockMgr instanceof ConcurrentLockManager)) {
                        out.print("cannot print");
                        return false;
                    }
                    out.print(((ConcurrentLockManager) lockMgr).printByResource());
                    return true;
                }

                @Override
                public String toString() {
                    return kind.name();
                }
            };
        }
        throw new AssertionError("Illegal Request Kind " + kind);
    }
}
