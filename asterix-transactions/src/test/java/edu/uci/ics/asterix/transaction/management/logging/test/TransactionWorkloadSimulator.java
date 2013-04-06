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
package edu.uci.ics.asterix.transaction.management.logging.test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.logging.BasicLogger;
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager.ResourceType;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobIdFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;

public class TransactionWorkloadSimulator {

    public static ILogManager logManager;
    public static ILockManager lockManager;
    TransactionSubsystem provider;

    public static WorkloadProperties workload;
    Transaction[] transactions;

    public TransactionWorkloadSimulator(WorkloadProperties workload) {
        this.workload = workload;
        transactions = new Transaction[workload.numActiveThreads];
    }

    public void beginWorkload() throws ACIDException {
        provider = new TransactionSubsystem("nc1", null);
        logManager = provider.getLogManager();
        lockManager = provider.getLockManager();
        provider.getTransactionalResourceRepository().registerTransactionalResourceManager(DummyResourceMgr.id,
                new DummyResourceMgr());
        Transaction[] transactions = new Transaction[workload.numActiveThreads];
        long startTime = System.nanoTime();
        for (int i = 0; i < workload.numActiveThreads; i++) {
            transactions[i] = new Transaction(provider, "Transaction " + (i + 1), workload.singleTransaction);
            transactions[i].start();
        }
        for (int i = 0; i < workload.numActiveThreads; i++) {
            try {
                transactions[i].join();
            } catch (InterruptedException ignore) {
            }
        }

        for (int i = 0; i < workload.numActiveThreads; i++) {
            provider.getTransactionManager().commitTransaction(transactions[i].getContext(), new DatasetId(-1), -1);
        }

        long endTime = System.nanoTime();
        int totalLogs = Transaction.logCount.get();
        System.out.println(" Total logs :" + totalLogs);
        long timeTaken = ((endTime - startTime) / 1000000);
        System.out.println(" total time :" + timeTaken);
        System.out.println(" throughput :" + totalLogs * 1000 / timeTaken + " logs/sec");
        long totalBytesWritten = Transaction.logByteCount.get();
        System.out.println(" bytes written :" + totalBytesWritten);
        System.out.println(" IO throughput " + totalBytesWritten * 1000 / timeTaken + " bytes/sec");
        System.out.println(" Avg Content Creation time :" + BasicLogger.getAverageContentCreationTime());
    }

    public static void main(String args[]) {
        WorkloadProperties workload = new WorkloadProperties();
        TransactionWorkloadSimulator simulator = new TransactionWorkloadSimulator(workload);
        try {
            simulator.beginWorkload();
        } catch (ACIDException acide) {
            acide.printStackTrace();
        }

    }
}

class SingleTransactionContextFactory {
    private static TransactionContext context;

    public static TransactionContext getContext(TransactionSubsystem provider) throws ACIDException {
        if (context == null) {
            context = new TransactionContext(JobIdFactory.generateJobId(), provider);
        }
        return context;
    }
}

class MultipleTransactionContextFactory {

    public static TransactionContext getContext(TransactionSubsystem provider) throws ACIDException {
        return new TransactionContext(JobIdFactory.generateJobId(), provider);
    }
}

class Transaction extends Thread {

    public static AtomicInteger logCount = new AtomicInteger(0);
    public static AtomicLong logByteCount = new AtomicLong(0);
    Random random = new Random();
    BasicLogger logger = new BasicLogger();
    LogicalLogLocator memLSN;
    String name;
    TransactionContext context;
    //private byte[] resourceID = new byte[1];
    private int resourceID;
    private int myLogCount = 0;
    private TransactionSubsystem transactionProvider;
    private ILogManager logManager;
    private DatasetId tempDatasetId = new DatasetId(-1);

    public Transaction(TransactionSubsystem provider, String name, boolean singleTransaction) throws ACIDException {
        this.name = name;
        this.transactionProvider = provider;
        if (singleTransaction) {
            context = SingleTransactionContextFactory.getContext(transactionProvider);
        } else {
            context = MultipleTransactionContextFactory.getContext(transactionProvider);
        }
        memLSN = LogUtil.getDummyLogicalLogLocator(transactionProvider.getLogManager());
        logManager = transactionProvider.getLogManager();
    }

    public TransactionContext getContext() {
        return context;
    }

    @Override
    public void run() {
        if (TransactionWorkloadSimulator.workload.minLogsPerTransactionThread == TransactionWorkloadSimulator.workload.maxLogsPerTransactionThread) {
            TransactionWorkloadSimulator.workload.maxLogsPerTransactionThread++;
        }
        int numLogs = TransactionWorkloadSimulator.workload.minLogsPerTransactionThread
                + random.nextInt(TransactionWorkloadSimulator.workload.maxLogsPerTransactionThread
                        - TransactionWorkloadSimulator.workload.minLogsPerTransactionThread);
        int total = 0;
        LogicalLogLocator memLSN = LogUtil.getDummyLogicalLogLocator(logManager);
        if (TransactionWorkloadSimulator.workload.maxLogSize == TransactionWorkloadSimulator.workload.minLogSize) {
            TransactionWorkloadSimulator.workload.maxLogSize++;
        }
        if (TransactionWorkloadSimulator.workload.singleResource) {
            int choice = random.nextInt(2);
            resourceID = (byte) (choice % 2);
        } else {
            random.nextInt(resourceID);
        }
        boolean retry = false;
        byte lockMode = -1;
        try {
            for (int i = 0; i < numLogs - 1; i++) {
                int logSize = TransactionWorkloadSimulator.workload.minLogSize
                        + random.nextInt(TransactionWorkloadSimulator.workload.maxLogSize
                                - TransactionWorkloadSimulator.workload.minLogSize);
                total += logSize;

                byte logType = LogType.UPDATE;
                byte logActionType = LogActionType.REDO_UNDO;
                long pageId = 0;
                if (!retry) {
                    lockMode = (byte)(random.nextInt(2));
                }
                tempDatasetId.setId(resourceID);
                TransactionWorkloadSimulator.lockManager.lock(tempDatasetId, -1, lockMode, context);
                TransactionWorkloadSimulator.logManager.log(logType, context, resourceID,
                        -1, resourceID, ResourceType.LSM_BTREE, logSize, null, logger, memLSN);
                retry = false;
                Thread.currentThread().sleep(TransactionWorkloadSimulator.workload.thinkTime);
                logCount.incrementAndGet();
                logByteCount.addAndGet(logSize
                        + TransactionWorkloadSimulator.logManager.getLogRecordHelper().getLogHeaderSize(logType)
                        + TransactionWorkloadSimulator.logManager.getLogRecordHelper().getLogChecksumSize());
                myLogCount++;
            }
        } catch (ACIDException acide) {
            acide.printStackTrace();
        } catch (Exception ie) {
            ie.printStackTrace();
        }
    }

}

class WorkloadProperties {
    public int numActiveThreads = 200;
    public long thinkTime = 0; // (in mesecs)
    public int minLogsPerTransactionThread = 5;
    public int maxLogsPerTransactionThread = 5;
    public int minLogSize = 1024 - 51;
    public int maxLogSize = 1024 - 51;
    public float commitFraction = 0.5f;
    public float rollbackFraction = 0.1f;
    public boolean singleTransaction = false;
    public boolean singleResource = true;
}

class ResourceMgrInfo {
    public static final byte BTreeResourceMgrId = 1;
    public static final byte MetadataResourceMgrId = 2;
}

class DummyResourceMgr implements IResourceManager {

    public static final byte id = 1;

    @Override
    public void redo(ILogRecordHelper logParser, LogicalLogLocator memLSN) throws ACIDException {
        // TODO Auto-generated method stub

    }

    @Override
    public void undo(ILogRecordHelper logParser, LogicalLogLocator memLSN) throws ACIDException {
        // TODO Auto-generated method stub

    }

    @Override
    public byte getResourceManagerId() {
        // TODO Auto-generated method stub
        return 1;
    }

}