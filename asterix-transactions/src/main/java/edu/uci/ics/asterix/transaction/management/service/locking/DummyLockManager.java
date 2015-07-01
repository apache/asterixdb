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
package edu.uci.ics.asterix.transaction.management.service.locking;

import java.io.IOException;
import java.io.OutputStream;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;


/**
 * A dummy implementation of the ILockManager interface. It assumes that all
 * requests are successful. It can be used to for jobs that are known to be
 * conflict free, but it'll yield terrible results if there are conflicts.
 *
 * @author tillw
 *
 */
public class DummyLockManager implements ILockManager, ILifeCycleComponent {

    public DummyLockManager(TransactionSubsystem transactionSubsystem) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
    }

    @Override
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException {
    }

    @Override
    public void unlock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
    }

    @Override
    public void instantLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext context)
            throws ACIDException {
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext context)
            throws ACIDException {
        return true;
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        return true;
    }

    @Override
    public String prettyPrint() throws ACIDException {
        return "DummyLockManager";
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
    }

}
