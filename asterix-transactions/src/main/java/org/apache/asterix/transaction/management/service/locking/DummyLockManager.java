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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;


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
