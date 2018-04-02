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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.transaction.management.service.locking.Request.Kind;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LockManagerUnitTest {

    public static int LOCK_MGR_SHRINK_TIMER = 5000;
    public static int LOCK_MGR_ARENAS = 2;
    public static int LOCK_MGR_TABLE_SIZE = 10;

    static int INITIAL_TIMESTAMP = 0;
    static long COORDINATOR_SLEEP = 20;
    static int TIMEOUT_MS = 100;

    Map<Integer, ITransactionContext> jobId2TxnCtxMap;
    ILockManager lockMgr;

    // set to e.g. System.err to get some output
    PrintStream out = System.out;
    PrintStream err = null; //System.err;

    //--------------------------------------------------------------------
    // JUnit methods
    //--------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        jobId2TxnCtxMap = new HashMap<>();
        lockMgr = new ConcurrentLockManager(LOCK_MGR_SHRINK_TIMER, LOCK_MGR_ARENAS, LOCK_MGR_TABLE_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        lockMgr = null;
        jobId2TxnCtxMap = null;
    }

    @Test
    public void testSimpleSharedUnlock() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.UNLOCK, j(1), d(1), e(1), LockMode.S));
        reportErrors(execute(reqs));
    }

    @Test
    public void testSimpleSharedRelease() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.RELEASE, j(1)));
        reportErrors(execute(reqs));
    }

    @Test
    public void testReacquire() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.RELEASE, j(1)));
        reportErrors(execute(reqs));
    }

    @Test
    public void testInstant() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.INSTANT_LOCK, j(1), d(1), e(1), LockMode.X));
        reqs.add(req(Kind.LOCK, j(2), d(1), e(1), LockMode.X));
        reqs.add(req(Kind.PRINT));
        reqs.add(req(Kind.INSTANT_LOCK, j(3), d(1), e(1), LockMode.S));
        expectError(execute(reqs), j(3), ACIDException.class);
    }

    @Test
    public void testTry() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.TRY_LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.LOCK, j(2), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.PRINT));
        reqs.add(req(Kind.TRY_LOCK, j(3), d(1), e(1), LockMode.X));
        reportErrors(execute(reqs));
    }

    @Test
    public void testInstantTry() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.INSTANT_LOCK, j(1), d(1), e(1), LockMode.X));
        reqs.add(req(Kind.LOCK, j(2), d(1), e(1), LockMode.X));
        reqs.add(req(Kind.PRINT));
        reqs.add(req(Kind.INSTANT_TRY_LOCK, j(3), d(1), e(1), LockMode.S));
        reportErrors(execute(reqs));
    }

    @Test
    /**
     * lock conversion/upgrade is not supported when deadlock-free locking
     * protocol is enabled.
     */
    public void testUpgrade() throws Exception {
        List<Request> reqs = new ArrayList<>();
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.S));
        reqs.add(req(Kind.LOCK, j(1), d(1), e(1), LockMode.X));
        reqs.add(req(Kind.RELEASE, j(1)));
        expectError(execute(reqs), j(1), IllegalStateException.class);
    }

    //--------------------------------------------------------------------
    // Helper methods
    //--------------------------------------------------------------------

    /**
     * Executes a list of requests where
     * a) each job runs in a different thread and
     * b) the threads/jobs are synchronized
     * The synchronization ensures that the requests are submitted to the
     * LockManager in list order, however they are fulfilled in the order
     * decided by the LockManager
     *
     * @param reqs
     *            a list of requests that will be execute in order
     * @return a map of (JodId, exception) pairs that can either be handled
     *         by the test or thrown using #reportErrors
     */
    private Map<String, Throwable> execute(List<Request> reqs) throws InterruptedException {
        if (err != null) {
            err.println("*** start ***");
        }
        final AtomicInteger timeStamp = new AtomicInteger(INITIAL_TIMESTAMP);
        Set<Locker> lockers = createLockers(reqs, timeStamp);
        Map<String, Thread> threads = startThreads(lockers);

        int coordinatorTime = timeStamp.get();
        while (active(lockers)) {
            if (err != null) {
                err.println("coordinatorTime = " + coordinatorTime);
            }
            if (coordinatorTime == timeStamp.get()) {
                Thread.sleep(COORDINATOR_SLEEP);
                if (coordinatorTime == timeStamp.get()) {
                    Locker timedOut = timedOut(lockers);
                    if (timedOut != null) {
                        if (err != null) {
                            err.println(timedOut.name + " timed out");
                        }
                        break;
                    }
                }
            }
            coordinatorTime = timeStamp.get();
        }
        Map<String, Throwable> result = stopThreads(lockers, threads);
        return result;
    }

    private boolean active(Set<Locker> lockers) {
        for (Locker locker : lockers) {
            if (locker.active()) {
                return true;
            }
        }
        return false;
    }

    private Locker timedOut(Set<Locker> lockers) {
        for (Locker locker : lockers) {
            if (locker.timedOut()) {
                return locker;
            }
        }
        return null;
    }

    private Set<Locker> createLockers(List<Request> reqs, AtomicInteger timeStamp) {
        Set<Locker> lockers = new HashSet<>();
        lockers.add(new Locker(lockMgr, null, reqs, timeStamp, err));
        for (ITransactionContext txnCtx : jobId2TxnCtxMap.values()) {
            Locker locker = new Locker(lockMgr, txnCtx, reqs, timeStamp, err);
            lockers.add(locker);
        }
        return lockers;
    }

    private Map<String, Thread> startThreads(Set<Locker> lockers) {
        Map<String, Thread> threads = new HashMap<>(lockers.size());
        for (Locker locker : lockers) {
            Thread t = new Thread(locker, locker.name);
            threads.put(locker.name, t);
            t.start();
        }
        return threads;
    }

    private Map<String, Throwable> stopThreads(Set<Locker> lockers, Map<String, Thread> threads)
            throws InterruptedException {
        Map<String, Throwable> result = new HashMap<>();
        for (Locker locker : lockers) {
            stopThread(threads.get(locker.name));
            List<Throwable> errors = locker.getErrors();
            if (errors != null) {
                errors.forEach(error -> result.put(locker.name, error));
            }
        }
        return result;
    }

    private void stopThread(Thread t) throws InterruptedException {
        if (err != null) {
            err.println("stopping " + t.getName() + " " + t.getState());
        }
        boolean done = false;
        while (!done) {
            switch (t.getState()) {
                case NEW:
                case RUNNABLE:
                case TERMINATED:
                    done = true;
                    break;
                default:
                    if (err != null) {
                        err.println("interrupting " + t.getName());
                    }
                    t.interrupt();
            }
        }
        if (err != null) {
            err.println("joining " + t.getName());
        }
        t.join();
    }

    /**
     * throws the first Throwable found in the map.
     * This is the default way to handle the errors returned by #execute
     *
     * @param errors
     *            a map of (JodId, exception) pairs
     */
    void reportErrors(Map<String, Throwable> errors) {
        for (String name : errors.keySet()) {
            throw new AssertionError("job " + name + " caught something", errors.get(name));
        }
        out.println("no errors");
    }

    void printErrors(Map<String, Throwable> errors) {
        errors.keySet().forEach(name -> out.println("Thread " + name + " caught " + errors.get(name)));
    }

    /**
     * gets the error for a specific job from the errors map
     *
     * @param errors
     *            a map of (JodId, throwable) pairs
     * @param txnCtx
     *            the transaction context of the job whose error is requested
     * @return throwable for said error
     */
    private static Throwable getError(Map<String, Throwable> errors, ITransactionContext txnCtx) {
        return errors.get(txnCtx.getTxnId().toString());
    }

    /**
     * asserts that the error for a specific job from the errors map is of a specific class
     *
     * @param errors
     *            a map of (JodId, throwable) pairs
     * @param txnCtx
     *            the transaction context of the job whose error is requested
     * @param clazz
     *            the exception class
     */
    private void expectError(Map<String, Throwable> errors, ITransactionContext txnCtx,
            Class<? extends Throwable> clazz) throws Exception {
        Throwable error = getError(errors, txnCtx);
        if (error == null) {
            throw new AssertionError(
                    "expected " + clazz.getSimpleName() + " for " + txnCtx.getTxnId() + ", got no " + "exception");
        }
        if (!clazz.isInstance(error)) {
            throw new AssertionError(error);
        }
        out.println("caught expected " + error);
    }

    //--------------------------------------------------------------------
    // Convenience methods to make test description more compact
    //--------------------------------------------------------------------

    private Request req(final Kind kind, final ITransactionContext txnCtx, final DatasetId dsId, final int hashValue,
            final byte lockMode) {
        return Request.create(kind, txnCtx, dsId, hashValue, lockMode);
    }

    private Request req(final Kind kind, final ITransactionContext txnCtx) {
        return Request.create(kind, txnCtx);
    }

    private Request req(final Kind kind) {
        return Request.create(kind, out);
    }

    private static DatasetId d(int id) {
        return new DatasetId(id);
    }

    private static int e(int i) {
        return i;
    }

    private ITransactionContext j(int jId) {
        if (!jobId2TxnCtxMap.containsKey(jId)) {
            ITransactionContext mockTxnContext = mock(ITransactionContext.class);
            when(mockTxnContext.getTxnId()).thenReturn(new TxnId(jId));
            jobId2TxnCtxMap.put(jId, mockTxnContext);
        }
        return jobId2TxnCtxMap.get(jId);
    }
}
