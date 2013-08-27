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

import java.util.ArrayList;
import java.util.Random;

import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;

/**
 * LockManagerUnitTest: unit test of LockManager
 * 
 * @author kisskys
 */

public class LockManagerRandomUnitTest {

    private static final int MAX_NUM_OF_UPGRADE_JOB = 2;//2
    private static final int MAX_NUM_OF_ENTITY_LOCK_JOB = 8;//8
    private static final int MAX_NUM_OF_DATASET_LOCK_JOB = 2;//2
    private static final int MAX_NUM_OF_THREAD_IN_A_JOB = 2; //4
    private static int jobId = 0;
    private static Random rand;

    public static void main(String args[]) throws ACIDException, AsterixException {
        int i;
        TransactionSubsystem txnProvider = new TransactionSubsystem("LockManagerRandomUnitTest", null,
                new AsterixTransactionProperties(new AsterixPropertiesAccessor()));
        rand = new Random(System.currentTimeMillis());
        for (i = 0; i < MAX_NUM_OF_ENTITY_LOCK_JOB; i++) {
            System.out.println("Creating " + i + "th EntityLockJob..");
            generateEntityLockThread(txnProvider);
        }

        for (i = 0; i < MAX_NUM_OF_DATASET_LOCK_JOB; i++) {
            System.out.println("Creating " + i + "th DatasetLockJob..");
            generateDatasetLockThread(txnProvider);
        }

        for (i = 0; i < MAX_NUM_OF_UPGRADE_JOB; i++) {
            System.out.println("Creating " + i + "th EntityLockUpgradeJob..");
            generateEntityLockUpgradeThread(txnProvider);
        }
    }

    private static void generateEntityLockThread(TransactionSubsystem txnProvider) {
        Thread t;
        int childCount = rand.nextInt(MAX_NUM_OF_THREAD_IN_A_JOB);
        if (MAX_NUM_OF_THREAD_IN_A_JOB != 0 && childCount == 0) {
            childCount = 1;
        }
        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (int i = 0; i < childCount; i++) {
            System.out.println("Creating " + txnContext.getJobId() + "," + i + "th EntityLockThread..");
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, false, false));
            t.start();
        }
    }

    private static void generateDatasetLockThread(TransactionSubsystem txnProvider) {
        Thread t;
        //        int childCount = rand.nextInt(MAX_NUM_OF_THREAD_IN_A_JOB);
        //        if (MAX_NUM_OF_THREAD_IN_A_JOB != 0 && childCount == 0) {
        //            childCount = 1;
        //        }
        int childCount = 1;

        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (int i = 0; i < childCount; i++) {
            System.out.println("Creating " + txnContext.getJobId() + "," + i + "th DatasetLockThread..");
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, true, false, false));
            t.start();
        }
    }

    private static void generateEntityLockUpgradeThread(TransactionSubsystem txnProvider) {
        int i;
        Thread t;
        int childCount = MAX_NUM_OF_THREAD_IN_A_JOB;
        if (MAX_NUM_OF_THREAD_IN_A_JOB != 0 && childCount == 0) {
            childCount = 1;
        }
        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (i = 0; i < childCount - 1; i++) {
            System.out.println("Creating " + txnContext.getJobId() + "," + i + "th EntityLockUpgradeThread(false)..");
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, true, false));
            t.start();
        }
        System.out.println("Creating " + txnContext.getJobId() + "," + i + "th EntityLockUpgradeThread(true)..");
        t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, true, true));
        t.start();
    }

    private static TransactionContext generateTxnContext(TransactionSubsystem txnProvider) {
        try {
            return new TransactionContext(new JobId(jobId++), txnProvider);
        } catch (ACIDException e) {
            e.printStackTrace();
            return null;
        }
    }

}

class LockRequestProducer implements Runnable {

    private static final int MAX_DATASET_NUM = 10;//10
    private static final int MAX_ENTITY_NUM = 30;//30
    private static final int MAX_LOCK_MODE_NUM = 2;
    private static final long DATASET_LOCK_THREAD_SLEEP_TIME = 1000;
    private static final int MAX_LOCK_REQUEST_TYPE_NUM = 4;

    private ILockManager lockMgr;
    private TransactionContext txnContext;
    private Random rand;
    private boolean isDatasetLock; //dataset or entity
    private ArrayList<LockRequest> requestQueue;
    private StringBuilder requestHistory;
    private int unlockIndex;
    private int upgradeIndex;
    private boolean isUpgradeThread;
    private boolean isUpgradeThreadJob;
    private boolean isDone;

    public LockRequestProducer(ILockManager lockMgr, TransactionContext txnContext, boolean isDatasetLock,
            boolean isUpgradeThreadJob, boolean isUpgradeThread) {
        this.lockMgr = lockMgr;
        this.txnContext = txnContext;
        this.isDatasetLock = isDatasetLock;
        this.isUpgradeThreadJob = isUpgradeThreadJob;
        this.isUpgradeThread = isUpgradeThread;

        this.rand = new Random(System.currentTimeMillis());
        requestQueue = new ArrayList<LockRequest>();
        requestHistory = new StringBuilder();
        unlockIndex = 0;
        upgradeIndex = 0;
        isDone = false;
    }

    @Override
    public void run() {
        try {
            if (isDatasetLock) {
                System.out.println("DatasetLockThread(" + Thread.currentThread().getName() + ") is running...");
                runDatasetLockTask();
            } else {
                System.out.println("EntityLockThread(" + Thread.currentThread().getName() + "," + isUpgradeThreadJob
                        + "," + isUpgradeThread + ") is running...");
                runEntityLockTask();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        } finally {

            /*
            System.out.println("" + Thread.currentThread().getName() + "\n" + requestHistory.toString() + ""
                    + Thread.currentThread().getName() + "\n");
            System.out.println("RequestHistoryPerJobId\n" + ((LockManager) lockMgr).getLocalRequestHistory());
            System.out.println("");
            System.out.println("GlobalRequestHistory\n" + ((LockManager) lockMgr).getGlobalRequestHistory());
            System.out.println("");
            */
        }
    }

    private void runDatasetLockTask() {
        try {
            produceDatasetLockRequest();
            if (isDone) {
                return;
            }
        } catch (ACIDException e) {
            e.printStackTrace();
            return;
        }

        try {
            Thread.sleep(DATASET_LOCK_THREAD_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            produceDatasetUnlockRequest();
            if (isDone) {
                return;
            }
        } catch (ACIDException e) {
            e.printStackTrace();
            return;
        }
    }

    private void runEntityLockTask() {
        int i;
        byte lockMode;
        int lockCount;
        int upgradeCount;
        int releaseCount;
        boolean mayRelease = false;

        lockCount = 1 + rand.nextInt(20);
        if (isUpgradeThreadJob) {
            if (isUpgradeThread) {
                upgradeCount = 1; //rand.nextInt(4) + 1;
                if (upgradeCount > lockCount) {
                    upgradeCount = lockCount;
                }
            } else {
                upgradeCount = 0;
            }
            lockMode = LockMode.S;
        } else {
            upgradeCount = 0;
            lockMode = (byte) (this.txnContext.getJobId().getId() % 2);
        }
        releaseCount = rand.nextInt(5) % 3 == 0 ? 1 : 0;

        //lock
        for (i = 0; i < lockCount; i++) {
            try {
                produceEntityLockRequest(lockMode);
                if (isDone) {
                    return;
                }
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }

        //upgrade
        for (i = 0; i < upgradeCount; i++) {
            try {
                produceEntityLockUpgradeRequest();
                if (isDone) {
                    return;
                }
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }

        //unlock or releaseLocks
        if (releaseCount == 0) {
            //unlock
            for (i = 0; i < lockCount; i++) {
                try {
                    produceEntityUnlockRequest();
                    if (isDone) {
                        return;
                    }
                } catch (ACIDException e) {
                    e.printStackTrace();
                    return;
                }
            }
        } else {
            try {
                synchronized (txnContext) {
                    if (txnContext.getTxnState() != ITransactionManager.ABORTED) {
                        txnContext.setTxnState(ITransactionManager.ABORTED);
                        mayRelease = true;
                    }
                }
                if (mayRelease) {
                    produceEntityReleaseLocksRequest();
                }
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void produceDatasetLockRequest() throws ACIDException {
        int requestType = RequestType.LOCK;
        int datasetId = rand.nextInt(MAX_DATASET_NUM);
        int entityHashValue = -1;
        byte lockMode = (byte) (rand.nextInt(MAX_LOCK_MODE_NUM));
        LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(datasetId),
                entityHashValue, lockMode, txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceDatasetUnlockRequest() throws ACIDException {
        LockRequest lockRequest = requestQueue.get(0);

        int requestType = RequestType.RELEASE_LOCKS;
        int datasetId = lockRequest.datasetIdObj.getId();
        int entityHashValue = -1;
        byte lockMode = LockMode.S;//lockMode is not used for unlock() call.
        LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(datasetId),
                entityHashValue, lockMode, txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceEntityLockRequest(byte lockMode) throws ACIDException {
        int requestType = rand.nextInt(MAX_LOCK_REQUEST_TYPE_NUM);
        int datasetId = rand.nextInt(MAX_DATASET_NUM);
        int entityHashValue = rand.nextInt(MAX_ENTITY_NUM);
        LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(datasetId),
                entityHashValue, lockMode, txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceEntityLockUpgradeRequest() throws ACIDException {
        LockRequest lockRequest = null;
        int size = requestQueue.size();
        boolean existLockRequest = false;

        while (upgradeIndex < size) {
            lockRequest = requestQueue.get(upgradeIndex++);
            if (lockRequest.isUpgrade || lockRequest.isTryLockFailed) {
                continue;
            }
            if (lockRequest.requestType == RequestType.UNLOCK || lockRequest.requestType == RequestType.RELEASE_LOCKS
                    || lockRequest.requestType == RequestType.INSTANT_LOCK
                    || lockRequest.requestType == RequestType.INSTANT_TRY_LOCK) {
                continue;
            }
            if (lockRequest.lockMode == LockMode.X) {
                continue;
            }
            existLockRequest = true;
            break;
        }

        if (existLockRequest) {
            int requestType = lockRequest.requestType;
            int datasetId = lockRequest.datasetIdObj.getId();
            int entityHashValue = lockRequest.entityHashValue;
            byte lockMode = LockMode.X;
            LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(
                    datasetId), entityHashValue, lockMode, txnContext);
            request.isUpgrade = true;
            requestQueue.add(request);
            requestHistory.append(request.prettyPrint());
            sendRequest(request);
        }
    }

    private void produceEntityUnlockRequest() throws ACIDException {
        LockRequest lockRequest = null;
        int size = requestQueue.size();
        boolean existLockRequest = false;

        while (unlockIndex < size) {
            lockRequest = requestQueue.get(unlockIndex++);
            if (lockRequest.isUpgrade || lockRequest.isTryLockFailed) {
                continue;
            }
            if (lockRequest.requestType == RequestType.UNLOCK || lockRequest.requestType == RequestType.RELEASE_LOCKS
                    || lockRequest.requestType == RequestType.INSTANT_LOCK
                    || lockRequest.requestType == RequestType.INSTANT_TRY_LOCK) {
                continue;
            }
            existLockRequest = true;
            break;
        }

        if (existLockRequest) {
            int requestType = RequestType.UNLOCK;
            int datasetId = lockRequest.datasetIdObj.getId();
            int entityHashValue = lockRequest.entityHashValue;
            byte lockMode = lockRequest.lockMode;
            LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(
                    datasetId), entityHashValue, lockMode, txnContext);
            requestQueue.add(request);
            requestHistory.append(request.prettyPrint());
            sendRequest(request);
        }
    }

    private void produceEntityReleaseLocksRequest() throws ACIDException {
        LockRequest lockRequest = null;
        int size = requestQueue.size();
        boolean existLockRequest = false;

        while (unlockIndex < size) {
            lockRequest = requestQueue.get(unlockIndex++);
            if (lockRequest.isUpgrade || lockRequest.isTryLockFailed) {
                continue;
            }
            if (lockRequest.requestType == RequestType.UNLOCK || lockRequest.requestType == RequestType.RELEASE_LOCKS
                    || lockRequest.requestType == RequestType.INSTANT_LOCK
                    || lockRequest.requestType == RequestType.INSTANT_TRY_LOCK) {
                continue;
            }
            existLockRequest = true;
            break;
        }

        if (existLockRequest) {
            int requestType = RequestType.RELEASE_LOCKS;
            int datasetId = lockRequest.datasetIdObj.getId();
            int entityHashValue = lockRequest.entityHashValue;
            byte lockMode = lockRequest.lockMode;
            LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, new DatasetId(
                    datasetId), entityHashValue, lockMode, txnContext);
            requestQueue.add(request);
            requestHistory.append(request.prettyPrint());
            sendRequest(request);
        }
    }

    private void sendRequest(LockRequest request) throws ACIDException {

        switch (request.requestType) {
            case RequestType.LOCK:
                try {
                    lockMgr.lock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
                } catch (ACIDException e) {
                    if (request.txnContext.isTimeout()) {
                        if (request.txnContext.getTxnState() != ITransactionManager.ABORTED) {
                            request.txnContext.setTxnState(ITransactionManager.ABORTED);
                            log("*** " + request.txnContext.getJobId() + " lock request causing deadlock ***");
                            log("Abort --> Releasing all locks acquired by " + request.txnContext.getJobId());
                            try {
                                lockMgr.releaseLocks(request.txnContext);
                            } catch (ACIDException e1) {
                                e1.printStackTrace();
                            }
                            log("Abort --> Released all locks acquired by " + request.txnContext.getJobId());
                        }
                        isDone = true;
                    } else {
                        throw e;
                    }
                }
                break;
            case RequestType.INSTANT_LOCK:
                try {
                    lockMgr.instantLock(request.datasetIdObj, request.entityHashValue, request.lockMode,
                            request.txnContext);
                } catch (ACIDException e) {
                    if (request.txnContext.isTimeout()) {
                        if (request.txnContext.getTxnState() != ITransactionManager.ABORTED) {
                            request.txnContext.setTxnState(ITransactionManager.ABORTED);
                            log("*** " + request.txnContext.getJobId() + " lock request causing deadlock ***");
                            log("Abort --> Releasing all locks acquired by " + request.txnContext.getJobId());
                            try {
                                lockMgr.releaseLocks(request.txnContext);
                            } catch (ACIDException e1) {
                                e1.printStackTrace();
                            }
                            log("Abort --> Released all locks acquired by " + request.txnContext.getJobId());
                        }
                        isDone = true;
                    } else {
                        throw e;
                    }
                }
                break;
            case RequestType.TRY_LOCK:
                request.isTryLockFailed = !lockMgr.tryLock(request.datasetIdObj, request.entityHashValue,
                        request.lockMode, request.txnContext);
                break;
            case RequestType.INSTANT_TRY_LOCK:
                lockMgr.instantTryLock(request.datasetIdObj, request.entityHashValue, request.lockMode,
                        request.txnContext);
                break;
            case RequestType.UNLOCK:
                lockMgr.unlock(request.datasetIdObj, request.entityHashValue, request.txnContext);
                break;
            case RequestType.RELEASE_LOCKS:
                lockMgr.releaseLocks(request.txnContext);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lock method");
        }
        try {
            Thread.sleep((long) 0);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void log(String s) {
        System.out.println(s);
    }
}

class LockRequest {
    public int requestType;
    public DatasetId datasetIdObj;
    public int entityHashValue;
    public byte lockMode;
    public ITransactionContext txnContext;
    public boolean isUpgrade;
    public boolean isTryLockFailed;
    public long requestTime;
    public String threadName;

    public LockRequest(String threadName, int requestType, DatasetId datasetIdObj, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) {
        this.requestType = requestType;
        this.datasetIdObj = datasetIdObj;
        this.entityHashValue = entityHashValue;
        this.lockMode = lockMode;
        this.txnContext = txnContext;
        this.requestTime = System.currentTimeMillis();
        this.threadName = new String(threadName);
        isUpgrade = false;
        isTryLockFailed = false;//used for TryLock request not to call Unlock when the tryLock failed.
    }

    public LockRequest(String threadName, int requestType) {
        this.requestType = requestType;
        this.requestTime = System.currentTimeMillis();
        this.threadName = new String(threadName);
    }

    //used for "W" request type
    public LockRequest(String threadName, int requestType, int waitTime) {
        this.requestType = requestType;
        this.requestTime = System.currentTimeMillis();
        this.threadName = new String(threadName);
        this.entityHashValue = waitTime;
    }

    public String prettyPrint() {
        StringBuilder s = new StringBuilder();
        //s.append(threadName.charAt(7)).append("\t").append("\t");
        s.append("T").append(threadName.substring(7)).append("\t");
        switch (requestType) {
            case RequestType.LOCK:
                s.append("L");
                break;
            case RequestType.TRY_LOCK:
                s.append("TL");
                break;
            case RequestType.INSTANT_LOCK:
                s.append("IL");
                break;
            case RequestType.INSTANT_TRY_LOCK:
                s.append("ITL");
                break;
            case RequestType.UNLOCK:
                s.append("UL");
                break;
            case RequestType.RELEASE_LOCKS:
                s.append("RL");
                break;
            case RequestType.CHECK_SEQUENCE:
                s.append("CSQ");
                return s.toString();
            case RequestType.CHECK_SET:
                s.append("CST");
                return s.toString();
            case RequestType.END:
                s.append("END");
                return s.toString();
            case RequestType.WAIT:
                s.append("W").append("\t").append(entityHashValue);
                return s.toString();
            default:
                throw new UnsupportedOperationException("Unsupported method");
        }
        s.append("\tJ").append(txnContext.getJobId().getId()).append("\tD").append(datasetIdObj.getId()).append("\tE")
                .append(entityHashValue).append("\t");
        switch (lockMode) {
            case LockMode.S:
                s.append("S");
                break;
            case LockMode.X:
                s.append("X");
                break;
            case LockMode.IS:
                s.append("IS");
                break;
            case LockMode.IX:
                s.append("IX");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lock mode");
        }
        s.append("\n");
        return s.toString();
    }
}

class RequestType {
    public static final int LOCK = 0;
    public static final int TRY_LOCK = 1;
    public static final int INSTANT_LOCK = 2;
    public static final int INSTANT_TRY_LOCK = 3;
    public static final int UNLOCK = 4;
    public static final int RELEASE_LOCKS = 5;
    public static final int CHECK_SEQUENCE = 6;
    public static final int CHECK_SET = 7;
    public static final int END = 8;
    public static final int WAIT = 9;
}
