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
package edu.uci.ics.asterix.transaction.management.test;

import java.io.IOException;
import java.util.Random;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.logging.IResource;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogger;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.recovery.IRecoveryManager;
import edu.uci.ics.asterix.transaction.management.service.recovery.IRecoveryManager.SystemState;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionIDFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public class TransactionSimulator {

    private ITransactionManager transactionManager;
    private ILogManager logManager;
    private ILockManager lockManager;
    private IRecoveryManager recoveryManager;
    private IResourceManager resourceMgr;
    private ILogger logger;
    private IResource resource;
    private LogicalLogLocator memLSN;
    private TransactionProvider transactionProvider;

    public TransactionSimulator(IResource resource, IResourceManager resourceMgr) throws ACIDException {
        String id = "nc1";
        transactionProvider = new TransactionProvider(id);
        transactionManager = transactionProvider.getTransactionManager();
        logManager = transactionProvider.getLogManager();
        lockManager = transactionProvider.getLockManager();
        recoveryManager = transactionProvider.getRecoveryManager();
        transactionProvider.getTransactionalResourceRepository().registerTransactionalResourceManager(
                resourceMgr.getResourceManagerId(), resourceMgr);
        this.resourceMgr = resourceMgr;
        this.logger = resource.getLogger();
        this.resource = resource;
        memLSN = LogUtil.getDummyLogicalLogLocator(transactionProvider.getLogManager());
    }

    public TransactionContext beginTransaction() throws ACIDException {
        long transactionId = TransactionIDFactory.generateTransactionId();
        return transactionManager.beginTransaction(transactionId);
    }

    public void executeTransactionOperation(TransactionContext txnContext, FileResource.CounterOperation operation)
            throws ACIDException {
        // lockManager.lock(txnContext, resourceId, 0);
        ILogManager logManager = transactionProvider.getLogManager();
        int currentValue = ((FileResource) resource).getMemoryCounter();
        int finalValue;
        switch (operation) {
            case INCREMENT:
                finalValue = currentValue + 1;
                int logRecordLength = ((FileLogger) logger).generateLogRecordContent(currentValue, finalValue);
                logManager.log(memLSN, txnContext, FileResourceManager.id, 0, LogType.UPDATE, LogActionType.REDO_UNDO,
                        logRecordLength, logger, null);
                ((FileResource) resource).increment();
                break;
            case DECREMENT:
                finalValue = currentValue - 1;
                logRecordLength = ((FileLogger) logger).generateLogRecordContent(currentValue, finalValue);
                logManager.log(memLSN, txnContext, FileResourceManager.id, 0, LogType.UPDATE, LogActionType.REDO_UNDO,
                        logRecordLength, logger, null);
                ((FileResource) resource).decrement();
                break;
        }

    }

    public void commitTransaction(TransactionContext context) throws ACIDException {
        transactionManager.commitTransaction(context);
    }

    public SystemState recover() throws ACIDException, IOException {
        SystemState state = recoveryManager.startRecovery(true);
        ((FileResource) resource).sync();
        return state;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, ACIDException {
        String fileDir = "testdata";
        String fileName = "counterFile";
        IResource resource = new FileResource(fileDir, fileName);
        FileResourceManager resourceMgr = new FileResourceManager();
        resourceMgr.registerTransactionalResource(resource);
        int existingValue = ((FileResource) resource).getDiskCounter();

        TransactionSimulator txnSimulator = new TransactionSimulator(((FileResource) resource), resourceMgr);
        int numTransactions = 2;
        Schedule schedule = new Schedule(numTransactions);

        for (int i = 0; i < numTransactions; i++) {
            TransactionContext context = txnSimulator.beginTransaction();
            txnSimulator.executeTransactionOperation(context, schedule.getOperations()[i]);
            if (schedule.getWillCommit()[i]) {
                txnSimulator.commitTransaction(context);
            }
        }

        int finalExpectedValue = existingValue + schedule.getDeltaChange();
        SystemState state = txnSimulator.recover();
        System.out.println(" State is " + state);
        boolean isCorrect = ((FileResource) resource).checkIfValueInSync(finalExpectedValue);
        System.out.println(" Did recovery happen correctly " + isCorrect);
    }

}

class ResourceMgrIds {

    public static final byte FileResourceMgrId = 1;

}

class Schedule {

    private int numCommittedIncrements;
    private int numCommittedDecrements;

    private FileResource.CounterOperation[] operations;
    private Boolean[] willCommit;

    public Boolean[] getWillCommit() {
        return willCommit;
    }

    private Random random = new Random();

    public int getDeltaChange() {
        return numCommittedIncrements - numCommittedDecrements;
    }

    public Schedule(int numTransactions) {
        operations = new FileResource.CounterOperation[numTransactions];
        willCommit = new Boolean[numTransactions];
        for (int i = 0; i < numTransactions; i++) {
            willCommit[i] = random.nextBoolean();
            int nextOp = random.nextInt(2);
            FileResource.CounterOperation op = nextOp == 0 ? FileResource.CounterOperation.INCREMENT
                    : FileResource.CounterOperation.DECREMENT;
            operations[i] = op;
            if (willCommit[i]) {
                if (op.equals(FileResource.CounterOperation.INCREMENT)) {
                    numCommittedIncrements++;
                } else {
                    numCommittedDecrements++;
                }
            }
        }

    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < operations.length; i++) {
            builder.append(" operation " + operations[i]);
            if (willCommit[i]) {
                builder.append(" commit ");
            } else {
                builder.append(" abort ");
            }
        }

        builder.append(" number of committed increments " + numCommittedIncrements);
        builder.append(" number of committed decrements " + numCommittedDecrements);
        return new String(builder);
    }

    public FileResource.CounterOperation[] getOperations() {
        return operations;
    }

}
