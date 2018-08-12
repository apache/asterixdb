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
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMaps;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * A concurrent implementation of the ILockManager interface.
 *
 * @see ResourceGroupTable
 * @see ResourceGroup
 */
@SuppressWarnings("squid:RedundantThrowsDeclarationCheck") // throws ACIDException
public class ConcurrentLockManager implements ILockManager, ILifeCycleComponent {

    static final Logger LOGGER = LogManager.getLogger();
    static final Level LVL = Level.TRACE;
    private static final boolean ENABLED_DEADLOCK_FREE_LOCKING_PROTOCOL = true;

    private static final int NIL = -1;
    private static final long NILL = -1L;

    private static final boolean DEBUG_MODE = false;//true
    private static final boolean CHECK_CONSISTENCY = false;

    private final ResourceGroupTable table;
    private final ResourceArenaManager resArenaMgr;
    private final RequestArenaManager reqArenaMgr;
    private final JobArenaManager jobArenaMgr;
    private final Long2LongMap txnId2TxnSlotMap;
    private final LockManagerStats stats = new LockManagerStats(10000);

    enum LockAction {
        ERR(false, false),
        GET(false, false),
        UPD(false, true), // version of GET that updates the max lock mode
        WAIT(true, false),
        CONV(true, true) // convert (upgrade) a lock (e.g. from S to X)
        ;
        boolean wait;
        boolean modify;

        LockAction(boolean wait, boolean modify) {
            this.wait = wait;
            this.modify = modify;
        }
    }

    private static final LockAction[][] ACTION_MATRIX = {
            // new    NL              IS               IX                S                X
            { LockAction.ERR, LockAction.UPD, LockAction.UPD, LockAction.UPD, LockAction.UPD }, // NL
            { LockAction.ERR, LockAction.GET, LockAction.UPD, LockAction.UPD, LockAction.WAIT }, // IS
            { LockAction.ERR, LockAction.GET, LockAction.GET, LockAction.WAIT, LockAction.WAIT }, // IX
            { LockAction.ERR, LockAction.GET, LockAction.WAIT, LockAction.GET, LockAction.WAIT }, // S
            { LockAction.ERR, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT } // X
    };

    public ConcurrentLockManager(final int lockManagerShrinkTimer) throws ACIDException {
        this(lockManagerShrinkTimer, Runtime.getRuntime().availableProcessors() * 2, 1024);
        // TODO increase table size?
    }

    public ConcurrentLockManager(final int lockManagerShrinkTimer, final int noArenas, final int tableSize)
            throws ACIDException {
        this.table = new ResourceGroupTable(tableSize);
        resArenaMgr = new ResourceArenaManager(noArenas, lockManagerShrinkTimer);
        reqArenaMgr = new RequestArenaManager(noArenas, lockManagerShrinkTimer);
        jobArenaMgr = new JobArenaManager(noArenas, lockManagerShrinkTimer);
        txnId2TxnSlotMap = Long2LongMaps.synchronize(new Long2LongOpenHashMap());
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        log("lock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.lock();

        final long txnId = txnContext.getTxnId().getId();
        final long jobSlot = findOrAllocJobSlot(txnId);
        final ResourceGroup group = table.get(datasetId.getId(), entityHashValue);
        group.getLatch();
        try {
            validateJob(txnContext);
            final long resSlot = findOrAllocResourceSlot(group, datasetId.getId(), entityHashValue);
            final long reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);
            boolean locked = false;
            while (!locked) {
                final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
                switch (act) {
                    case CONV:
                        if (introducesDeadlock(resSlot, jobSlot, NOPTracker.INSTANCE)) {
                            DeadlockTracker tracker = new CollectingTracker();
                            tracker.pushJob(jobSlot);
                            introducesDeadlock(resSlot, jobSlot, tracker);
                            requestAbort(txnContext, tracker.toString());
                            break;
                        } else if (hasOtherHolders(resSlot, jobSlot)) {
                            enqueueWaiter(group, reqSlot, resSlot, jobSlot, act, txnContext);
                            break;
                        }
                        //fall-through
                    case UPD:
                        resArenaMgr.setMaxMode(resSlot, lockMode);
                        //fall-through
                    case GET:
                        addHolder(reqSlot, resSlot, jobSlot);
                        locked = true;
                        break;
                    case WAIT:
                        enqueueWaiter(group, reqSlot, resSlot, jobSlot, act, txnContext);
                        break;
                    case ERR:
                    default:
                        throw new IllegalStateException();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ACIDException(e);
        } finally {
            group.releaseLatch();
        }

        if (CHECK_CONSISTENCY) {
            assertLocksCanBefoundInJobQueue();
        }
    }

    private void enqueueWaiter(final ResourceGroup group, final long reqSlot, final long resSlot, final long jobSlot,
            final LockAction act, ITransactionContext txnContext) throws ACIDException, InterruptedException {
        final Queue queue = act.modify ? upgrader : waiter;
        if (introducesDeadlock(resSlot, jobSlot, NOPTracker.INSTANCE)) {
            DeadlockTracker tracker = new CollectingTracker();
            tracker.pushJob(jobSlot);
            introducesDeadlock(resSlot, jobSlot, tracker);
            requestAbort(txnContext, tracker.toString());
        } else {
            queue.add(reqSlot, resSlot, jobSlot);
        }
        try {
            group.await(txnContext);
        } finally {
            queue.remove(reqSlot, resSlot, jobSlot);
        }
    }

    interface DeadlockTracker {
        void pushResource(long resSlot);

        void pushRequest(long reqSlot);

        void pushJob(long jobSlot);

        void pop();
    }

    private static class NOPTracker implements DeadlockTracker {
        static final DeadlockTracker INSTANCE = new NOPTracker();

        @Override
        public void pushResource(long resSlot) {
            // no-op
        }

        @Override
        public void pushRequest(long reqSlot) {
            // no-op
        }

        @Override
        public void pushJob(long jobSlot) {
            // no-op
        }

        @Override
        public void pop() {
            // no-op
        }
    }

    private static class CollectingTracker implements DeadlockTracker {

        static final boolean DEBUG = false;

        LongList slots = new LongArrayList();
        ArrayList<String> types = new ArrayList<>();

        @Override
        public void pushResource(long resSlot) {
            types.add("Resource");
            slots.add(resSlot);
            if (DEBUG) {
                LOGGER.info("push " + types.get(types.size() - 1) + " " + slots.getLong(slots.size() - 1));
            }
        }

        @Override
        public void pushRequest(long reqSlot) {
            types.add("Request");
            slots.add(reqSlot);
            if (DEBUG) {
                LOGGER.info("push " + types.get(types.size() - 1) + " " + slots.getLong(slots.size() - 1));
            }
        }

        @Override
        public void pushJob(long jobSlot) {
            types.add("Job");
            slots.add(jobSlot);
            if (DEBUG) {
                LOGGER.info("push " + types.get(types.size() - 1) + " " + slots.getLong(slots.size() - 1));
            }
        }

        @Override
        public void pop() {
            if (DEBUG) {
                LOGGER.info("pop " + types.get(types.size() - 1) + " " + slots.getLong(slots.size() - 1));
            }
            types.remove(types.size() - 1);
            slots.removeLong(slots.size() - 1);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < slots.size(); ++i) {
                sb.append(types.get(i)).append(" ").append(TypeUtil.Global.toString(slots.getLong(i))).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * determine if adding a job to the waiters of a resource will introduce a
     * cycle in the wait-graph where the job waits on itself - but not directly on itself (which happens e.g. in the
     * case of upgrading a lock from S to X).
     *
     * @param resSlot
     *            the slot that contains the information about the resource
     * @param jobSlot
     *            the slot that contains the information about the job
     * @return true if a cycle would be introduced, false otherwise
     */
    private boolean introducesDeadlock(final long resSlot, final long jobSlot, final DeadlockTracker tracker) {
        /*
         * Due to the deadlock-free locking protocol, deadlock is not possible.
         * So, this method always returns false in that case
         */
        return !ENABLED_DEADLOCK_FREE_LOCKING_PROTOCOL && introducesDeadlock(resSlot, jobSlot, tracker, 0);
    }

    private boolean introducesDeadlock(final long resSlot, final long jobSlot, final DeadlockTracker tracker,
            final int depth) {
        synchronized (jobArenaMgr) {
            tracker.pushResource(resSlot);
            long reqSlot = resArenaMgr.getLastHolder(resSlot);
            while (reqSlot >= 0) {
                tracker.pushRequest(reqSlot);
                final long holderJobSlot = reqArenaMgr.getJobSlot(reqSlot);
                tracker.pushJob(holderJobSlot);
                if (holderJobSlot == jobSlot && depth != 0) {
                    return true;
                }

                // To determine if we have a deadlock we need to look at the waiters and at the upgraders.
                // The scanWaiters flag indicates if we are currently scanning the waiters (true) or the upgraders
                // (false).
                boolean scanWaiters = true;
                long jobWaiter = jobArenaMgr.getLastWaiter(holderJobSlot);
                if (jobWaiter < 0) {
                    scanWaiters = false;
                    jobWaiter = jobArenaMgr.getLastUpgrader(holderJobSlot);
                }
                while (jobWaiter >= 0) {
                    long waitingOnResSlot = reqArenaMgr.getResourceId(jobWaiter);
                    if (introducesDeadlock(waitingOnResSlot, jobSlot, tracker, depth + 1)) {
                        return true;
                    }
                    jobWaiter = reqArenaMgr.getNextJobRequest(jobWaiter);
                    if (jobWaiter < 0 && scanWaiters) {
                        scanWaiters = false;
                        jobWaiter = jobArenaMgr.getLastUpgrader(holderJobSlot);
                    }
                }

                tracker.pop(); // job
                tracker.pop(); // request
                reqSlot = reqArenaMgr.getNextRequest(reqSlot);
            }
            tracker.pop(); // resource
            return false;
        }
    }

    @Override
    public void instantLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        log("instantLock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.instantLock();

        final long txnId = txnContext.getTxnId().getId();
        final ResourceGroup group = table.get(datasetId.getId(), entityHashValue);
        if (group.firstResourceIndex.get() == NILL) {
            validateJob(txnContext);
            // if we do not have a resource in the group, we know that the
            // resource that we are looking for is not locked
            return;
        }

        // we only allocate a request slot if we actually have to wait
        long reqSlot = NILL;

        group.getLatch();
        try {
            validateJob(txnContext);
            final long resSlot = findResourceInGroup(group, datasetId.getId(), entityHashValue);
            if (resSlot < 0) {
                // if we don't find the resource, there are no locks on it.
                return;
            }

            final long jobSlot = findOrAllocJobSlot(txnId);

            while (true) {
                final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
                switch (act) {
                    case UPD:
                    case GET:
                        return;
                    case WAIT:
                    case CONV:
                        if (reqSlot == NILL) {
                            reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);
                        }
                        enqueueWaiter(group, reqSlot, resSlot, jobSlot, act, txnContext);
                        break;
                    case ERR:
                    default:
                        throw new IllegalStateException();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ACIDException(e);
        } finally {
            if (reqSlot != NILL) {
                // deallocate request, if we allocated one earlier
                if (DEBUG_MODE) {
                    LOGGER.trace("del req slot " + TypeUtil.Global.toString(reqSlot));
                }
                reqArenaMgr.deallocate(reqSlot);
            }
            group.releaseLatch();
        }
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        log("tryLock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.tryLock();

        final long txnId = txnContext.getTxnId().getId();
        final long jobSlot = findOrAllocJobSlot(txnId);
        final ResourceGroup group = table.get(datasetId.getId(), entityHashValue);
        group.getLatch();

        try {
            validateJob(txnContext);

            final long resSlot = findOrAllocResourceSlot(group, datasetId.getId(), entityHashValue);
            final long reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);

            final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
            switch (act) {
                case UPD:
                    resArenaMgr.setMaxMode(resSlot, lockMode);
                    //fall-through
                case GET:
                    addHolder(reqSlot, resSlot, jobSlot);
                    return true;
                case WAIT:
                case CONV:
                    return false;
                default:
                    throw new IllegalStateException();
            }
        } finally {
            group.releaseLatch();
        }
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        log("instantTryLock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.instantTryLock();

        final long txnId = txnContext.getTxnId().getId();
        final ResourceGroup group = table.get(datasetId.getId(), entityHashValue);
        if (group.firstResourceIndex.get() == NILL) {
            validateJob(txnContext);
            // if we do not have a resource in the group, we know that the
            // resource that we are looking for is not locked
            return true;
        }

        group.getLatch();
        try {
            validateJob(txnContext);

            final long resSlot = findResourceInGroup(group, datasetId.getId(), entityHashValue);
            if (resSlot < 0) {
                // if we don't find the resource, there are no locks on it.
                return true;
            }

            final long jobSlot = findOrAllocJobSlot(txnId);

            LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
            switch (act) {
                case UPD:
                case GET:
                    return true;
                case WAIT:
                case CONV:
                    return false;
                case ERR:
                default:
                    throw new IllegalStateException();
            }
        } finally {
            group.releaseLatch();
        }
    }

    @Override
    public void unlock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        log("unlock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        final long txnId = txnContext.getTxnId().getId();
        final long jobSlot = txnId2TxnSlotMap.get(txnId);

        unlock(datasetId.getId(), entityHashValue, lockMode, jobSlot);
    }

    private void unlock(int dsId, int entityHashValue, byte lockMode, long jobSlot) throws ACIDException {
        log("unlock", dsId, entityHashValue, lockMode, null);
        stats.unlock();

        ResourceGroup group = table.get(dsId, entityHashValue);
        group.getLatch();
        try {

            long resource = findResourceInGroup(group, dsId, entityHashValue);
            if (resource < 0) {
                throw new IllegalStateException("resource (" + dsId + ",  " + entityHashValue + ") not found");
            }

            if (CHECK_CONSISTENCY) {
                assertLocksCanBefoundInJobQueue();
            }

            long holder = removeLastHolder(resource, jobSlot, lockMode);

            // deallocate request
            if (DEBUG_MODE) {
                LOGGER.trace("del req slot " + TypeUtil.Global.toString(holder));
            }
            reqArenaMgr.deallocate(holder);
            // deallocate resource or fix max lock mode
            if (resourceNotUsed(resource)) {
                long prev = group.firstResourceIndex.get();
                if (prev == resource) {
                    group.firstResourceIndex.set(resArenaMgr.getNext(resource));
                } else {
                    while (resArenaMgr.getNext(prev) != resource) {
                        prev = resArenaMgr.getNext(prev);
                    }
                    resArenaMgr.setNext(prev, resArenaMgr.getNext(resource));
                }
                if (DEBUG_MODE) {
                    LOGGER.trace("del res slot " + TypeUtil.Global.toString(resource));
                }
                resArenaMgr.deallocate(resource);
            } else {
                final int oldMaxMode = resArenaMgr.getMaxMode(resource);
                final int newMaxMode = determineNewMaxMode(resource, oldMaxMode);
                resArenaMgr.setMaxMode(resource, newMaxMode);
                group.wakeUp();
            }
        } finally {
            group.releaseLatch();
        }
    }

    @Override
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException {
        log("releaseLocks", NIL, NIL, LockMode.ANY, txnContext);
        stats.releaseLocks();

        long txnId = txnContext.getTxnId().getId();
        long jobSlot = txnId2TxnSlotMap.get(txnId);
        if (jobSlot == 0) {
            // we don't know the job, so there are no locks for it - we're done
            return;
        }
        //System.err.println(table.append(new StringBuilder(), true).toString());
        if (LOGGER.isEnabled(LVL)) {
            LOGGER.log(LVL, "jobArenaMgr " + jobArenaMgr.addTo(new RecordManagerStats()).toString());
            LOGGER.log(LVL, "resArenaMgr " + resArenaMgr.addTo(new RecordManagerStats()).toString());
            LOGGER.log(LVL, "reqArenaMgr " + reqArenaMgr.addTo(new RecordManagerStats()).toString());
        }
        long holder;
        synchronized (jobArenaMgr) {
            holder = jobArenaMgr.getLastHolder(jobSlot);
        }
        while (holder != NILL) {
            long resource = reqArenaMgr.getResourceId(holder);
            int dsId = resArenaMgr.getDatasetId(resource);
            int pkHashVal = resArenaMgr.getPkHashVal(resource);
            unlock(dsId, pkHashVal, LockMode.ANY, jobSlot);
            synchronized (jobArenaMgr) {
                holder = jobArenaMgr.getLastHolder(jobSlot);
            }
        }
        if (DEBUG_MODE) {
            LOGGER.trace("del job slot " + TypeUtil.Global.toString(jobSlot));
        }
        jobArenaMgr.deallocate(jobSlot);
        txnId2TxnSlotMap.remove(txnId);
        stats.logCounters(LOGGER, Level.DEBUG, true);
    }

    private long findOrAllocJobSlot(long txnId) {
        long jobSlot = txnId2TxnSlotMap.get(txnId);
        if (jobSlot == 0) {
            jobSlot = jobArenaMgr.allocate();
            if (DEBUG_MODE) {
                LOGGER.trace("new job slot " + TypeUtil.Global.toString(jobSlot) + " (" + txnId + ")");
            }
            jobArenaMgr.setTxnId(jobSlot, txnId);
            long oldSlot = txnId2TxnSlotMap.putIfAbsent(txnId, jobSlot);
            if (oldSlot != 0) {
                // if another thread allocated a slot for this jobThreadId between
                // get(..) and putIfAbsent(..), we'll use that slot and
                // deallocate the one we allocated
                if (DEBUG_MODE) {
                    LOGGER.trace("del job slot " + TypeUtil.Global.toString(jobSlot) + " due to conflict");
                }
                jobArenaMgr.deallocate(jobSlot);
                jobSlot = oldSlot;
            }
        }
        assert jobSlot > 0;
        return jobSlot;
    }

    private long findOrAllocResourceSlot(ResourceGroup group, int dsId, int entityHashValue) {
        long resSlot = findResourceInGroup(group, dsId, entityHashValue);

        if (resSlot == NILL) {
            // we don't know about this resource, let's alloc a slot
            resSlot = resArenaMgr.allocate();
            resArenaMgr.setDatasetId(resSlot, dsId);
            resArenaMgr.setPkHashVal(resSlot, entityHashValue);
            resArenaMgr.setNext(resSlot, group.firstResourceIndex.get());
            group.firstResourceIndex.set(resSlot);
            if (DEBUG_MODE) {
                LOGGER.trace("new res slot " + TypeUtil.Global.toString(resSlot) + " (" + dsId + ", " + entityHashValue
                        + ")");
            }
        } else {
            if (DEBUG_MODE) {
                LOGGER.trace("fnd res slot " + TypeUtil.Global.toString(resSlot) + " (" + dsId + ", " + entityHashValue
                        + ")");
            }
        }
        return resSlot;
    }

    private long allocRequestSlot(long resSlot, long jobSlot, byte lockMode) {
        long reqSlot = reqArenaMgr.allocate();
        reqArenaMgr.setResourceId(reqSlot, resSlot);
        reqArenaMgr.setLockMode(reqSlot, lockMode); // lock mode is a byte!!
        reqArenaMgr.setJobSlot(reqSlot, jobSlot);
        if (DEBUG_MODE) {
            LOGGER.trace("new req slot " + TypeUtil.Global.toString(reqSlot) + " (" + TypeUtil.Global.toString(resSlot)
                    + ", " + TypeUtil.Global.toString(jobSlot) + ", " + LockMode.toString(lockMode) + ")");
        }
        return reqSlot;
    }

    private LockAction determineLockAction(long resSlot, long jobSlot, byte lockMode) {
        final int curLockMode = resArenaMgr.getMaxMode(resSlot);
        final LockAction act = ACTION_MATRIX[curLockMode][lockMode];
        if (act == LockAction.WAIT) {
            return updateActionForSameJob(resSlot, jobSlot, lockMode);
        }
        return act;
    }

    /**
     * when we've got a lock conflict for a different job, we always have to
     * wait, if it is for the same job we either have to
     * a) (wait and) convert the lock once conversion becomes viable or
     * b) acquire the lock if we want to lock the same resource with the same
     * lock mode for the same job.
     *
     * @param resource
     *            the resource slot that's being locked
     * @param job
     *            the job slot of the job locking the resource
     * @param lockMode
     *            the lock mode that the resource should be locked with
     * @return
     */
    private LockAction updateActionForSameJob(long resource, long job, byte lockMode) {
        // TODO we can reduce the number of things we have to look at by
        // carefully distinguishing the different lock modes
        long holder = resArenaMgr.getLastHolder(resource);
        LockAction res = LockAction.WAIT;
        while (holder != NILL) {
            if (job == reqArenaMgr.getJobSlot(holder)) {
                if (reqArenaMgr.getLockMode(holder) == lockMode) {
                    return LockAction.GET;
                } else {
                    if (ENABLED_DEADLOCK_FREE_LOCKING_PROTOCOL) {
                        throw new IllegalStateException(
                                "Lock conversion is not supported when deadlock-free locking protocol is enabled!");
                    }
                    res = LockAction.CONV;
                }
            }
            holder = reqArenaMgr.getNextRequest(holder);
        }
        return res;
    }

    private long findResourceInGroup(ResourceGroup group, int dsId, int entityHashValue) {
        stats.logCounters(LOGGER, LVL, false);
        long resSlot = group.firstResourceIndex.get();
        while (resSlot != NILL) {
            // either we already have a lock on this resource or we have a
            // hash collision
            if (resArenaMgr.getDatasetId(resSlot) == dsId && resArenaMgr.getPkHashVal(resSlot) == entityHashValue) {
                return resSlot;
            } else {
                resSlot = resArenaMgr.getNext(resSlot);
            }
        }
        return NILL;
    }

    private void addHolder(long request, long resource, long job) {
        long lastHolder = resArenaMgr.getLastHolder(resource);
        reqArenaMgr.setNextRequest(request, lastHolder);
        resArenaMgr.setLastHolder(resource, request);

        synchronized (jobArenaMgr) {
            long lastJobHolder = jobArenaMgr.getLastHolder(job);
            insertIntoJobQueue(request, lastJobHolder);
            jobArenaMgr.setLastHolder(job, request);
        }
    }

    private boolean hasOtherHolders(long resSlot, long jobSlot) {
        long holder = resArenaMgr.getLastHolder(resSlot);
        while (holder != NILL) {
            if (reqArenaMgr.getJobSlot(holder) != jobSlot) {
                return true;
            }
            holder = reqArenaMgr.getNextRequest(holder);
        }
        return false;
    }

    private long removeLastHolder(long resource, long jobSlot, byte lockMode) {
        long holder = resArenaMgr.getLastHolder(resource);
        if (holder < 0) {
            throw new IllegalStateException("no holder for resource " + resource);
        }
        // remove from the list of holders for a resource
        if (requestMatches(holder, jobSlot, lockMode)) {
            // if the head of the queue matches, we need to update the resource
            long next = reqArenaMgr.getNextRequest(holder);
            resArenaMgr.setLastHolder(resource, next);
        } else {
            holder = removeRequestFromQueueForJob(holder, jobSlot, lockMode);
        }

        synchronized (jobArenaMgr) {
            // remove from the list of requests for a job
            long newHead = removeRequestFromJob(holder, jobArenaMgr.getLastHolder(jobSlot));
            jobArenaMgr.setLastHolder(jobSlot, newHead);
        }
        return holder;
    }

    private boolean requestMatches(long holder, long jobSlot, byte lockMode) {
        return jobSlot == reqArenaMgr.getJobSlot(holder)
                && (lockMode == LockMode.ANY || lockMode == reqArenaMgr.getLockMode(holder));
    }

    private long removeRequestFromJob(long holder, long unmodified) {
        long prevForJob = reqArenaMgr.getPrevJobRequest(holder);
        long nextForJob = reqArenaMgr.getNextJobRequest(holder);
        if (nextForJob != NILL) {
            reqArenaMgr.setPrevJobRequest(nextForJob, prevForJob);
        }
        if (prevForJob == NILL) {
            return nextForJob;
        } else {
            reqArenaMgr.setNextJobRequest(prevForJob, nextForJob);
            return unmodified;
        }
    }

    interface Queue {
        void add(long request, long resource, long job);

        void remove(long request, long resource, long job);
    }

    private final Queue waiter = new Queue() {
        @Override
        public void add(long request, long resource, long job) {
            long waiter = resArenaMgr.getFirstWaiter(resource);
            reqArenaMgr.setNextRequest(request, NILL);
            if (waiter == NILL) {
                resArenaMgr.setFirstWaiter(resource, request);
            } else {
                appendToRequestQueue(waiter, request);
            }
            synchronized (jobArenaMgr) {
                waiter = jobArenaMgr.getLastWaiter(job);
                insertIntoJobQueue(request, waiter);
                jobArenaMgr.setLastWaiter(job, request);
            }
        }

        @Override
        public void remove(long request, long resource, long job) {
            long waiter = resArenaMgr.getFirstWaiter(resource);
            if (waiter == request) {
                long next = reqArenaMgr.getNextRequest(waiter);
                resArenaMgr.setFirstWaiter(resource, next);
            } else {
                waiter = removeRequestFromQueueForSlot(waiter, request);
            }
            synchronized (jobArenaMgr) {
                // remove from the list of requests for a job
                long newHead = removeRequestFromJob(waiter, jobArenaMgr.getLastWaiter(job));
                jobArenaMgr.setLastWaiter(job, newHead);
            }
        }
    };

    private final Queue upgrader = new Queue() {
        @Override
        public void add(long request, long resource, long job) {
            long upgrader = resArenaMgr.getFirstUpgrader(resource);
            reqArenaMgr.setNextRequest(request, NILL);
            if (upgrader == NILL) {
                resArenaMgr.setFirstUpgrader(resource, request);
            } else {
                appendToRequestQueue(upgrader, request);
            }
            synchronized (jobArenaMgr) {
                upgrader = jobArenaMgr.getLastUpgrader(job);
                insertIntoJobQueue(request, upgrader);
                jobArenaMgr.setLastUpgrader(job, request);
            }
        }

        @Override
        public void remove(long request, long resource, long job) {
            long upgrader = resArenaMgr.getFirstUpgrader(resource);
            if (upgrader == request) {
                long next = reqArenaMgr.getNextRequest(upgrader);
                resArenaMgr.setFirstUpgrader(resource, next);
            } else {
                upgrader = removeRequestFromQueueForSlot(upgrader, request);
            }
            synchronized (jobArenaMgr) {
                // remove from the list of requests for a job
                long newHead = removeRequestFromJob(upgrader, jobArenaMgr.getLastUpgrader(job));
                jobArenaMgr.setLastUpgrader(job, newHead);
            }
        }
    };

    private void insertIntoJobQueue(long newRequest, long oldRequest) {
        reqArenaMgr.setNextJobRequest(newRequest, oldRequest);
        reqArenaMgr.setPrevJobRequest(newRequest, NILL);
        if (oldRequest >= 0) {
            reqArenaMgr.setPrevJobRequest(oldRequest, newRequest);
        }
    }

    private void appendToRequestQueue(long head, long appendee) {
        long next = reqArenaMgr.getNextRequest(head);
        while (next != NILL) {
            head = next;
            next = reqArenaMgr.getNextRequest(head);
        }
        reqArenaMgr.setNextRequest(head, appendee);
    }

    private long removeRequestFromQueueForSlot(long head, long reqSlot) {
        long cur = head;
        long prev = cur;
        while (prev != NILL) {
            cur = reqArenaMgr.getNextRequest(prev);
            if (cur == NILL) {
                throw new IllegalStateException("request " + reqSlot + " not in queue");
            }
            if (cur == reqSlot) {
                break;
            }
            prev = cur;
        }
        long next = reqArenaMgr.getNextRequest(cur);
        reqArenaMgr.setNextRequest(prev, next);
        return cur;
    }

    /**
     * remove the first request for a given job and lock mode from a request queue.
     * If the value of the parameter lockMode is LockMode.ANY the first request
     * for the job is removed - independent of the LockMode.
     *
     * @param head
     *            the head of the request queue
     * @param jobSlot
     *            the job slot
     * @param lockMode
     *            the lock mode
     * @return the slot of the first request that matched the given job
     */
    private long removeRequestFromQueueForJob(long head, long jobSlot, byte lockMode) {
        long holder = head;
        long prev = holder;
        while (prev != NILL) {
            holder = reqArenaMgr.getNextRequest(prev);
            if (holder == NILL) {
                throw new IllegalStateException("no entry for job " + jobSlot + " in queue");
            }
            if (requestMatches(holder, jobSlot, lockMode)) {
                break;
            }
            prev = holder;
        }
        long next = reqArenaMgr.getNextRequest(holder);
        reqArenaMgr.setNextRequest(prev, next);
        return holder;
    }

    private int determineNewMaxMode(long resource, int oldMaxMode) {
        int newMaxMode = LockMode.NL;
        long holder = resArenaMgr.getLastHolder(resource);
        while (holder != NILL) {
            int curLockMode = reqArenaMgr.getLockMode(holder);
            if (curLockMode == oldMaxMode) {
                // we have another lock of the same mode - we're done
                return oldMaxMode;
            }
            switch (ACTION_MATRIX[newMaxMode][curLockMode]) {
                case UPD:
                    newMaxMode = curLockMode;
                    break;
                case GET:
                    break;
                case WAIT:
                case CONV:
                case ERR:
                    throw new IllegalStateException("incompatible locks in holder queue");
            }
            holder = reqArenaMgr.getNextRequest(holder);
        }
        return newMaxMode;
    }

    private boolean resourceNotUsed(long resource) {
        return resArenaMgr.getLastHolder(resource) == NILL && resArenaMgr.getFirstUpgrader(resource) == NILL
                && resArenaMgr.getFirstWaiter(resource) == NILL;
    }

    private void validateJob(ITransactionContext txnContext) throws ACIDException {
        if (txnContext.getTxnState() == ITransactionManager.ABORTED) {
            throw new ACIDException("" + txnContext.getTxnId() + " is in ABORTED state.");
        } else if (txnContext.isTimeout()) {
            requestAbort(txnContext, "timeout");
        }
    }

    private void requestAbort(ITransactionContext txnContext, String msg) throws ACIDException {
        txnContext.setTimeout(true);
        throw new ACIDException(
                "Transaction " + txnContext.getTxnId() + " should abort (requested by the Lock Manager)" + ":\n" + msg);
    }

    /*
     * Debugging support
     */

    private void log(String string, int id, int entityHashValue, byte lockMode, ITransactionContext txnContext) {
        if (!LOGGER.isEnabled(LVL)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{ op : ").append(string);
        if (id != NIL) {
            sb.append(" , dataset : ").append(id);
        }
        if (entityHashValue != NIL) {
            sb.append(" , entity : ").append(entityHashValue);
        }
        if (lockMode != LockMode.NL) {
            sb.append(" , mode : ").append(LockMode.toString(lockMode));
        }
        if (txnContext != null) {
            sb.append(" , txnId : ").append(txnContext.getTxnId());
        }
        sb.append(" , thread : ").append(Thread.currentThread().getName());
        sb.append(" }");
        LOGGER.log(LVL, sb.toString());
    }

    private void assertLocksCanBefoundInJobQueue() throws ACIDException {
        try {
            for (int i = 0; i < table.size; ++i) {
                final ResourceGroup group = table.get(i);
                if (group.tryLatch(100, TimeUnit.MILLISECONDS)) {
                    try {
                        long resSlot = group.firstResourceIndex.get();
                        while (resSlot != NILL) {
                            int dsId = resArenaMgr.getDatasetId(resSlot);
                            int entityHashValue = resArenaMgr.getPkHashVal(resSlot);
                            long reqSlot = resArenaMgr.getLastHolder(resSlot);
                            while (reqSlot != NILL) {
                                byte lockMode = (byte) reqArenaMgr.getLockMode(reqSlot);
                                long jobSlot = reqArenaMgr.getJobSlot(reqSlot);
                                long txnId = jobArenaMgr.getTxnId(jobSlot);
                                assertLockCanBeFoundInJobQueue(dsId, entityHashValue, lockMode, txnId);
                                reqSlot = reqArenaMgr.getNextRequest(reqSlot);
                            }
                            resSlot = resArenaMgr.getNext(resSlot);
                        }
                    } finally {
                        group.releaseLatch();
                    }
                } else {
                    LOGGER.warn("Could not check locks for " + group);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private void assertLockCanBeFoundInJobQueue(int dsId, int entityHashValue, byte lockMode, long txnId) {
        if (findLockInJobQueue(dsId, entityHashValue, txnId, lockMode) == NILL) {
            String msg = "request for " + LockMode.toString(lockMode) + " lock on dataset " + dsId + " entity "
                    + entityHashValue + " not found for txn " + txnId + " in thread "
                    + Thread.currentThread().getName();
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * tries to find a lock request searching though the job queue
     *
     * @param dsId
     *            dataset id
     * @param entityHashValue
     *            primary key hash value
     * @param txnId
     *            job id
     * @param lockMode
     *            lock mode
     * @return the slot of the request, if the lock request is found, NILL otherwise
     */
    private long findLockInJobQueue(final int dsId, final int entityHashValue, final long txnId, byte lockMode) {
        long jobSlot = txnId2TxnSlotMap.get(txnId);
        if (jobSlot == 0) {
            return NILL;
        }

        long holder;
        synchronized (jobArenaMgr) {
            holder = jobArenaMgr.getLastHolder(jobSlot);
        }
        while (holder != NILL) {
            long resource = reqArenaMgr.getResourceId(holder);
            if (dsId == resArenaMgr.getDatasetId(resource) && entityHashValue == resArenaMgr.getPkHashVal(resource)
                    && jobSlot == reqArenaMgr.getJobSlot(holder)
                    && (lockMode == reqArenaMgr.getLockMode(holder) || lockMode == LockMode.ANY)) {
                return holder;
            }
            synchronized (jobArenaMgr) {
                holder = reqArenaMgr.getNextJobRequest(holder);
            }
        }
        return NILL;
    }

    private TablePrinter getResourceTablePrinter() {
        return new ResourceTablePrinter(table, resArenaMgr, reqArenaMgr, jobArenaMgr);
    }

    private TablePrinter getDumpTablePrinter() {
        return new DumpTablePrinter(table, resArenaMgr, reqArenaMgr, jobArenaMgr, txnId2TxnSlotMap);
    }

    public String printByResource() {
        return getResourceTablePrinter().append(new StringBuilder()).append("\n").toString();
    }

    @Override
    public String toString() {
        return printByResource();
    }

    public String dump() {
        return getDumpTablePrinter().append(new StringBuilder()).toString();
    }

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return getDumpTablePrinter().append(s).toString() + "\n";
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        os.write(dump().getBytes());
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) throws IOException {
        if (dumpState) {
            dumpState(os);
        }
    }
}
