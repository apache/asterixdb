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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.junit.Assert;

/**
 * Executes a sequence of lock requests against an ILockManager.
 * Lockers are run by different threads in the LockManagerUnitTest.
 *
 * @see ILockManager
 * @see LockManagerUnitTest
 */
class Locker implements Runnable {

    public String name;

    private ILockManager lockMgr;

    private List<Requester> requests;
    private Iterator<Requester> reqIter;
    private volatile Requester curReq;
    private int reqStart;

    private AtomicInteger globalTime;
    private List<Throwable> errors;

    private PrintStream err;

    /**
     * @param lockMgr
     *            the ILockManager to send requests to
     * @param txnCtx
     *            the ITransactionContext that identifies the transaction that this Locker represents
     * @param allRequests
     *            an ordered list of lock requests for multiple transactions, this Locker will only execute
     *            requests for the transaction identified by txnCtx
     * @param time
     *            a global timestamp that is used to synchronize different lockers to ensure that requests are started
     *            in the order given in allRequests
     * @param err
     *            a stream to write log/error information to
     * @see Request
     */
    Locker(ILockManager lockMgr, ITransactionContext txnCtx, List<Request> allRequests, AtomicInteger time,
            PrintStream err) {
        this.name = txnCtx == null ? "admin" : txnCtx.getTxnId().toString();
        this.lockMgr = lockMgr;

        this.requests = new LinkedList<>();
        for (int pos = 0; pos < allRequests.size(); ++pos) {
            Request req = allRequests.get(pos);
            if (req.txnCtx == txnCtx) {
                requests.add(new Requester(pos, req));
            }
        }
        this.reqIter = requests.iterator();
        this.globalTime = time;
        this.err = err;
    }

    private boolean hasErrors() {
        return errors != null && errors.size() > 0;
    }

    synchronized List<Throwable> getErrors() {
        return errors;
    }

    private synchronized void addError(Throwable error) {
        log("caught " + error);
        if (this.errors == null) {
            this.errors = Collections.synchronizedList(new ArrayList<Throwable>());
        }
        this.errors.add(error);
    }

    public synchronized boolean active() {
        return !hasErrors() && (reqIter.hasNext() || curReq != null);
    }

    public synchronized boolean timedOut() {
        return reqStart > 0 && (currentTime() - reqStart) > LockManagerUnitTest.TIMEOUT_MS;
    }

    @Override
    public void run() {
        log("running");
        try {
            while (!hasErrors() && reqIter.hasNext()) {
                curReq = reqIter.next();
                int localTime = globalTime.get();
                while (localTime < curReq.time) {
                    Thread.sleep(10);
                    localTime = globalTime.get();
                }
                if (localTime != curReq.time) {
                    throw new AssertionError("missed time for request " + curReq);
                }
                log("will exec at t=" + localTime + " " + curReq);
                try {
                    reqStart = currentTime();
                    Assert.assertEquals(localTime, globalTime.get());
                    curReq.setResult(curReq.request.execute(lockMgr) ? Requester.SUCCESS : Requester.FAIL);
                } catch (ACIDException e) {
                    curReq.setResult(Requester.ERROR);
                    addError(e);
                } finally {
                    globalTime.getAndIncrement();
                    log("incremented");
                    reqStart = -1;
                }
                log("time " + localTime);
            }
            curReq = null;
        } catch (InterruptedException ie) {
            log("got interrupted");
        } catch (Throwable e) {
            if (err != null) {
                e.printStackTrace(err);
            }
            addError(e);
        }
        log("done");
    }

    private void log(String msg) {
        if (err != null) {
            err.println(Thread.currentThread().getName() + " " + msg);
        }
    }

    private static int currentTime() {
        return ((int) System.currentTimeMillis()) & 0x7fffffff;
    }

    public String toString() {
        return "[" + name + "]" + curReq;
    }
}

class Requester {

    public static byte NONE = -1;
    public static byte FAIL = 0;
    public static byte SUCCESS = 1;
    public static byte ERROR = 2;

    int time;
    Request request;
    byte result = NONE;

    Requester(int time, Request request) {
        this.time = time;
        this.request = request;
    }

    void setResult(byte res) {
        result = res;
    }

    public String toString() {
        return request.toString() + " t=" + time;
    }
}
