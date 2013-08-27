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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;

public class LockManagerDeterministicUnitTest {

    public static void main(String args[]) throws ACIDException, IOException, AsterixException {
        //prepare configuration file
        File cwd = new File(System.getProperty("user.dir"));
        File asterixdbDir = cwd.getParentFile();
        File srcFile = new File(asterixdbDir.getAbsoluteFile(), "asterix-app/src/main/resources/asterix-build-configuration.xml");
        File destFile = new File(cwd, "target/classes/asterix-configuration.xml");
        FileUtils.copyFile(srcFile, destFile);

        //initialize controller thread
        String requestFileName = new String(
                "src/main/java/edu/uci/ics/asterix/transaction/management/service/locking/LockRequestFile");
        Thread t = new Thread(new LockRequestController(requestFileName));
        t.start();
    }
}

class LockRequestController implements Runnable {

    public static final boolean IS_DEBUG_MODE = false;
    TransactionSubsystem txnProvider;
    WorkerReadyQueue workerReadyQueue;
    ArrayList<LockRequest> requestList;
    ArrayList<ArrayList<Integer>> expectedResultList;
    int resultListIndex;
    LockManager lockMgr;
    String requestFileName;
    long defaultWaitTime;

    public LockRequestController(String requestFileName) throws ACIDException, AsterixException {
        this.txnProvider = new TransactionSubsystem("nc1", null, new AsterixTransactionProperties(
                new AsterixPropertiesAccessor()));
        this.workerReadyQueue = new WorkerReadyQueue();
        this.requestList = new ArrayList<LockRequest>();
        this.expectedResultList = new ArrayList<ArrayList<Integer>>();
        this.lockMgr = (LockManager) txnProvider.getLockManager();
        this.requestFileName = new String(requestFileName);
        this.resultListIndex = 0;
        this.defaultWaitTime = 10;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Thread-0");
        HashMap<String, Thread> threadMap = new HashMap<String, Thread>();
        Thread t = null;
        LockRequest lockRequest = null;
        boolean isSuccess = true;

        try {
            readRequest();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ACIDException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        //initialize workerThread
        int size = requestList.size();
        for (int i = 0; i < size; i++) {
            lockRequest = requestList.get(i);
            if (lockRequest.threadName.equals("Thread-0")) {
                //Thread-0 is controller thread.
                continue;
            }
            t = threadMap.get(lockRequest.threadName);
            if (t == null) {
                t = new Thread(new LockRequestWorker(txnProvider, workerReadyQueue, lockRequest.threadName),
                        lockRequest.threadName);
                threadMap.put(lockRequest.threadName, t);
                t.start();
                log("Created " + lockRequest.threadName);
            }
        }

        //wait for all workerThreads to be ready
        try {
            log("waiting for all workerThreads to complete initialization ...");
            Thread.sleep(5);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        while (workerReadyQueue.size() != threadMap.size()) {
            try {
                log(" .");
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //make workerThread work
        while (requestList.size() != 0) {
            lockRequest = requestList.remove(0);
            log("Processing: " + lockRequest.prettyPrint());
            try {
                if (!handleRequest(lockRequest)) {
                    log("\n*** Test Failed ***");
                    isSuccess = false;
                    break;
                } else {
                    log("Processed: " + lockRequest.prettyPrint());
                }
            } catch (ACIDException e) {
                e.printStackTrace();
                break;
            }
        }

        if (isSuccess) {
            log("\n*** Test Passed ***");
        }
    }

    public boolean handleRequest(LockRequest request) throws ACIDException {
        LockRequestWorker worker = null;
        int i = 0;

        if (request.requestType == RequestType.CHECK_SEQUENCE) {
            return validateExpectedResult(true);
        } else if (request.requestType == RequestType.CHECK_SET) {
            return validateExpectedResult(false);
        } else if (request.requestType == RequestType.WAIT) {
            try {
                Thread.sleep((long) request.entityHashValue);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        } else if (request.requestType == RequestType.END) {
            worker = workerReadyQueue.pop(request.threadName);
            while (worker == null) {
                if (!IS_DEBUG_MODE) {
                    log(request.threadName + " is not in the workerReadyQueue");
                    return false;
                }
                log(Thread.currentThread().getName() + " waiting for " + request.threadName
                        + " to be in the workerReadyQueue[" + i++ + "].");
                try {
                    Thread.sleep((long) 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
                worker = workerReadyQueue.pop(request.threadName);
            }
            synchronized (worker) {
                worker.setDone(true);
                worker.setWait(false);
                worker.notify();
            }
            try {
                Thread.sleep((long) defaultWaitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            worker = workerReadyQueue.pop(request.threadName);
            while (worker == null) {
                if (!IS_DEBUG_MODE) {
                    log(request.threadName + " is not in the workerReadyQueue");
                    return false;
                }
                log(Thread.currentThread().getName() + " waiting for " + request.threadName
                        + " to be in the workerReadyQueue[" + i++ + "].");
                try {
                    Thread.sleep((long) 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                worker = workerReadyQueue.pop(request.threadName);
            }

            synchronized (worker) {
                worker.setLockRequest(request);
                worker.setWait(false);
                worker.notify();
            }

            try {
                Thread.sleep((long) defaultWaitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public boolean validateExpectedResult(boolean isSequence) {

        if (isSequence) {
            return workerReadyQueue.checkSequence(expectedResultList.get(resultListIndex++));
        } else {
            return workerReadyQueue.checkSet(expectedResultList.get(resultListIndex++));
        }

    }

    public void readRequest() throws IOException, ACIDException {
        int i = 0;
        LockRequest lockRequest = null;
        TransactionContext txnContext = null;
        HashMap<Integer, TransactionContext> jobMap = new HashMap<Integer, TransactionContext>();

        int threadId;
        String requestType;
        int jobId;
        int datasetId;
        int PKHashVal;
        int waitTime;
        ArrayList<Integer> list = null;
        String lockMode;

        Scanner scanner = new Scanner(new FileInputStream(requestFileName));
        while (scanner.hasNextLine()) {
            try {
                threadId = Integer.parseInt(scanner.next().substring(1));
                requestType = scanner.next();
                if (requestType.equals("CSQ") || requestType.equals("CST") || requestType.equals("END")) {
                    log("LockRequest[" + i++ + "]:T" + threadId + "," + requestType);
                    lockRequest = new LockRequest("Thread-" + threadId, getRequestType(requestType));
                    if (requestType.equals("CSQ") || requestType.equals("CST")) {
                        list = new ArrayList<Integer>();
                        while (scanner.hasNextInt()) {
                            threadId = scanner.nextInt();
                            if (threadId < 0) {
                                break;
                            }
                            list.add(threadId);
                        }
                        expectedResultList.add(list);
                    }
                } else if (requestType.equals("DW")) {
                    defaultWaitTime = scanner.nextInt();
                    log("LockRequest[" + i++ + "]:T" + threadId + "," + requestType + "," + defaultWaitTime);
                    continue;
                } else if (requestType.equals("W")) {
                    waitTime = scanner.nextInt();
                    log("LockRequest[" + i++ + "]:T" + threadId + "," + requestType);
                    lockRequest = new LockRequest("Thread-" + threadId, getRequestType(requestType), waitTime);
                } else {
                    jobId = Integer.parseInt(scanner.next().substring(1));
                    datasetId = Integer.parseInt(scanner.next().substring(1));
                    PKHashVal = Integer.parseInt(scanner.next().substring(1));
                    lockMode = scanner.next();
                    txnContext = jobMap.get(jobId);
                    if (txnContext == null) {
                        txnContext = new TransactionContext(new JobId(jobId), txnProvider);
                        jobMap.put(jobId, txnContext);
                    }
                    log("LockRequest[" + i++ + "]:T" + threadId + "," + requestType + ",J" + jobId + ",D" + datasetId
                            + ",E" + PKHashVal + "," + lockMode);
                    lockRequest = new LockRequest("Thread-" + threadId, getRequestType(requestType), new DatasetId(
                            datasetId), PKHashVal, getLockMode(lockMode), txnContext);
                }

                requestList.add(lockRequest);
            } catch (NoSuchElementException e) {
                scanner.close();
                break;
            }
        }
    }

    public void log(String s) {
        System.out.println(s);
    }

    private int getRequestType(String s) {
        if (s.equals("L")) {
            return RequestType.LOCK;
        }

        if (s.equals("TL")) {
            return RequestType.TRY_LOCK;
        }

        if (s.equals("IL")) {
            return RequestType.INSTANT_LOCK;
        }

        if (s.equals("ITL")) {
            return RequestType.INSTANT_TRY_LOCK;
        }

        if (s.equals("UL")) {
            return RequestType.UNLOCK;
        }

        if (s.equals("RL")) {
            return RequestType.RELEASE_LOCKS;
        }

        if (s.equals("CSQ")) {
            return RequestType.CHECK_SEQUENCE;
        }

        if (s.equals("CST")) {
            return RequestType.CHECK_SET;
        }

        if (s.equals("END")) {
            return RequestType.END;
        }

        if (s.equals("W")) {
            return RequestType.WAIT;
        }

        try {
            throw new UnsupportedOperationException("Invalid request type:" + s);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }

        return -1;

    }

    private byte getLockMode(String s) {
        if (s.equals("S")) {
            return LockMode.S;
        }

        if (s.equals("X")) {
            return LockMode.X;
        }

        try {
            throw new UnsupportedOperationException("Invalid lock mode type:" + s);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }

        return -1;
    }
}

class LockRequestWorker implements Runnable {

    String threadName;
    TransactionSubsystem txnProvider;
    ILockManager lockMgr;
    WorkerReadyQueue workerReadyQueue;
    LockRequest lockRequest;
    boolean needWait;
    boolean isAwaken;
    boolean isDone;

    public LockRequestWorker(TransactionSubsystem txnProvider, WorkerReadyQueue workerReadyQueue, String threadName) {
        this.txnProvider = txnProvider;
        this.lockMgr = txnProvider.getLockManager();
        this.workerReadyQueue = workerReadyQueue;
        this.threadName = new String(threadName);
        this.lockRequest = null;
        needWait = true;
        isDone = false;
        isAwaken = false;
    }

    public boolean isAwaken() {
        return isAwaken;
    }

    @Override
    public void run() {
        //initial wait
        needWait = true;
        isAwaken = false;

        while (!isDone) {
            while (needWait) {
                synchronized (this) {
                    workerReadyQueue.push(this);
                    try {
                        this.wait();
                        isAwaken = true;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (isDone) {
                break;
            }

            try {
                sendRequest(lockRequest);
            } catch (ACIDException e) {
                if (lockRequest.txnContext.isTimeout()) {
                    if (lockRequest.txnContext.getTxnState() != ITransactionManager.ABORTED) {
                        lockRequest.txnContext.setTxnState(ITransactionManager.ABORTED);
                        log("*** " + lockRequest.txnContext.getJobId() + " lock request causing deadlock ***");
                        log("Abort --> Releasing all locks acquired by " + lockRequest.txnContext.getJobId());
                        try {
                            lockMgr.releaseLocks(lockRequest.txnContext);
                        } catch (ACIDException e1) {
                            e1.printStackTrace();
                        }
                        log("Abort --> Released all locks acquired by " + lockRequest.txnContext.getJobId());
                    }
                    isDone = true;
                } else {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            needWait = true;
            isAwaken = false;
        }
    }

    public void sendRequest(LockRequest request) throws ACIDException {

        switch (request.requestType) {
            case RequestType.LOCK:
                lockMgr.lock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
                break;
            case RequestType.INSTANT_LOCK:
                lockMgr.instantLock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
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
    }

    public void setLockRequest(LockRequest request) {
        this.lockRequest = request;
    }

    public void setWait(boolean wait) {
        needWait = wait;
    }

    public void setDone(boolean done) {
        isDone = done;
    }

    public String getThreadName() {
        return threadName;
    }

    public void log(String s) {
        System.out.println(s);
    }
}

class WorkerReadyQueue {
    ArrayList<LockRequestWorker> workerReadyQueue;

    public WorkerReadyQueue() {
        workerReadyQueue = new ArrayList<LockRequestWorker>();
    }

    public synchronized void push(LockRequestWorker worker) {
        workerReadyQueue.add(worker);
    }

    public synchronized LockRequestWorker pop(String threadName) {
        int i;
        LockRequestWorker worker = null;
        int size = workerReadyQueue.size();
        for (i = 0; i < size; i++) {
            worker = workerReadyQueue.get(i);
            if (worker.getThreadName().equals(threadName)) {
                workerReadyQueue.remove(i);
                break;
            }
        }

        if (i == size) {
            return null;
        } else {
            return worker;
        }
    }

    public synchronized int size() {
        return workerReadyQueue.size();
    }

    public boolean checkSet(ArrayList<Integer> threadIdList) {
        int i;
        int j;
        StringBuilder s = new StringBuilder();
        LockRequestWorker worker = null;
        int resultListSize = 0;
        int queueSize = workerReadyQueue.size();
        int listSize = threadIdList.size();

        s.append("ExpectedList(Set):\t");
        for (i = 0; i < listSize; i++) {
            s.append(threadIdList.get(i)).append(" ");
        }
        s.append("\n");

        while (queueSize < listSize) {
            //wait until workers finish its task
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log(Thread.currentThread().getName() + " waiting for worker to finish its task...");
            queueSize = workerReadyQueue.size();
        }

        if (listSize != queueSize) {
            log("listSize:" + listSize + ", queueSize:" + queueSize);
            return false;
        }

        s.append("ResultList(Set):\t");
        for (i = 0; i < listSize; i++) {
            for (j = 0; j < queueSize; j++) {
                worker = workerReadyQueue.get(j);
                if (worker.getThreadName().equals("Thread-" + threadIdList.get(i))) {
                    s.append(threadIdList.get(i)).append(" ");
                    resultListSize++;
                    break;
                }
            }
        }

        log(s.toString());
        if (listSize != resultListSize) {
            return false;
        }

        return true;
    }

    public boolean checkSequence(ArrayList<Integer> threadIdList) {
        int i;
        StringBuilder s = new StringBuilder();
        LockRequestWorker worker = null;
        int queueSize = workerReadyQueue.size();
        int listSize = threadIdList.size();

        s.append("ExpectedList(Sequence):\t");
        for (i = 0; i < listSize; i++) {
            s.append(threadIdList.get(i)).append(" ");
        }
        s.append("\n");

        while (queueSize < listSize) {
            //wait until workers finish its task
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log(Thread.currentThread().getName() + "Waiting for worker to finish its task...");
            queueSize = workerReadyQueue.size();
        }

        if (queueSize != listSize) {
            return false;
        }

        s.append("ResultList(Sequence):\t");
        for (i = 0; i < listSize; i++) {
            worker = workerReadyQueue.get(i);
            if (!worker.getThreadName().equals("Thread-" + threadIdList.get(i))) {
                log(s.toString());
                return false;
            } else {
                s.append(threadIdList.get(i)).append(" ");
            }
        }

        log(s.toString());
        return true;
    }

    public void log(String s) {
        System.out.println(s);
    }
}
