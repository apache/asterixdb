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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the ILockManager interface.
 *
 * @author tillw
 */
public class ConcurrentLockManager implements ILockManager, ILifeCycleComponent {

    private static final Logger LOGGER
        = Logger.getLogger(ConcurrentLockManager.class.getName());
    private static final Level LVL = Level.FINER;
    
    public static final boolean DEBUG_MODE = false;//true
    public static final boolean CHECK_CONSISTENCY = false;

    private TransactionSubsystem txnSubsystem;
    private ResourceGroupTable table;
    private ResourceArenaManager resArenaMgr;
    private RequestArenaManager reqArenaMgr;
    private JobArenaManager jobArenaMgr;
    private ConcurrentHashMap<Integer, Long> jobIdSlotMap;
    private ThreadLocal<DatasetLockCache> dsLockCache;
    private LockManagerStats stats = new LockManagerStats(10000); 
    
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

    static LockAction[][] ACTION_MATRIX = {
            // new    NL              IS               IX                S                X
            { LockAction.ERR, LockAction.UPD, LockAction.UPD, LockAction.UPD, LockAction.UPD }, // NL
            { LockAction.ERR, LockAction.GET, LockAction.UPD, LockAction.UPD, LockAction.WAIT }, // IS
            { LockAction.ERR, LockAction.GET, LockAction.GET, LockAction.WAIT, LockAction.WAIT }, // IX
            { LockAction.ERR, LockAction.GET, LockAction.WAIT, LockAction.GET, LockAction.WAIT }, // S
            { LockAction.ERR, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT } // X
    };

    public ConcurrentLockManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;

        this.table = new ResourceGroupTable();

        final int lockManagerShrinkTimer = txnSubsystem.getTransactionProperties().getLockManagerShrinkTimer();

        int noArenas = Runtime.getRuntime().availableProcessors() * 2;

        resArenaMgr = new ResourceArenaManager(noArenas, lockManagerShrinkTimer);
        reqArenaMgr = new RequestArenaManager(noArenas, lockManagerShrinkTimer);
        jobArenaMgr = new JobArenaManager(noArenas, lockManagerShrinkTimer);
        jobIdSlotMap = new ConcurrentHashMap<>();
        dsLockCache = new ThreadLocal<DatasetLockCache>() {
            protected DatasetLockCache initialValue() {
                return new DatasetLockCache();
            }
        };
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return this.txnSubsystem.getTransactionProperties();
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        log("lock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.lock();
        
        final int dsId = datasetId.getId();        
        final int jobId = txnContext.getJobId().getId();

        if (entityHashValue != -1) {
            lock(datasetId, -1, LockMode.intentionMode(lockMode), txnContext);
        } else {
            if (dsLockCache.get().contains(jobId, dsId, lockMode)) {
                return;
            }
        }

        final long jobSlot = findOrAllocJobSlot(jobId);
        
        final ResourceGroup group = table.get(dsId, entityHashValue);
        group.getLatch();
        try {
            validateJob(txnContext);

            final long resSlot = findOrAllocResourceSlot(group, dsId, entityHashValue);
            final long reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);
            boolean locked = false;
            while (!locked) {
                final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
                switch (act) {
                    case UPD:
                        resArenaMgr.setMaxMode(resSlot, lockMode);
                        // no break
                    case GET:
                        addHolder(reqSlot, resSlot, jobSlot);
                        locked = true;
                        break;
                    case WAIT:
                    case CONV:
                        enqueueWaiter(group, reqSlot, resSlot, jobSlot, act, txnContext);
                        break;
                    case ERR:
                    default:
                        throw new IllegalStateException();
                }
            }
            if (entityHashValue == -1) {
                dsLockCache.get().put(jobId, dsId, lockMode);
            }
        } finally {
            group.releaseLatch();
        }
        
        if (CHECK_CONSISTENCY) assertLocksCanBefoundInJobQueue();
    }

    private void enqueueWaiter(final ResourceGroup group, final long reqSlot, final long resSlot, final long jobSlot,
            final LockAction act, ITransactionContext txnContext) throws ACIDException {
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
    
    static class NOPTracker implements DeadlockTracker {        
        static final DeadlockTracker INSTANCE = new NOPTracker();

        public void pushResource(long resSlot) {}
        public void pushRequest(long reqSlot) {}
        public void pushJob(long jobSlot) {}
        public void pop() {}
    }
    
    static class CollectingTracker implements DeadlockTracker {
        ArrayList<Long> slots = new ArrayList<Long>();
        ArrayList<String> types = new ArrayList<String>();

        @Override
        public void pushResource(long resSlot) {
            types.add("Resource");
            slots.add(resSlot);
            System.err.println("push " + types.get(types.size() - 1) + " " + slots.get(slots.size() - 1));
        }

        @Override
        public void pushRequest(long reqSlot) {
            types.add("Request");
            slots.add(reqSlot);
            System.err.println("push " + types.get(types.size() - 1) + " " + slots.get(slots.size() - 1));
        }

        @Override
        public void pushJob(long jobSlot) {
            types.add("Job");
            slots.add(jobSlot);
            System.err.println("push " + types.get(types.size() - 1) + " " + slots.get(slots.size() - 1));
        }

        @Override
        public void pop() {
            System.err.println("pop " + types.get(types.size() - 1) + " " + slots.get(slots.size() - 1));
            types.remove(types.size() - 1);
            slots.remove(slots.size() - 1);            
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < slots.size(); ++i) {
                sb.append(types.get(i) + " " + TypeUtil.Global.toString(slots.get(i)) + "\n");
            }
            return sb.toString();
        }
    }
        
    /**
     * determine if adding a job to the waiters of a resource will introduce a
     * cycle in the wait-graph where the job waits on itself
     * 
     * @param resSlot
     *            the slot that contains the information about the resource
     * @param jobSlot
     *            the slot that contains the information about the job
     * @return true if a cycle would be introduced, false otherwise
     */
    private boolean introducesDeadlock(final long resSlot, final long jobSlot,
            final DeadlockTracker tracker) {
        synchronized (jobArenaMgr) {
            tracker.pushResource(resSlot);
            long reqSlot = resArenaMgr.getLastHolder(resSlot);
            while (reqSlot >= 0) {
                tracker.pushRequest(reqSlot);
                final long holderJobSlot = reqArenaMgr.getJobSlot(reqSlot);
                tracker.pushJob(holderJobSlot);
                if (holderJobSlot == jobSlot) {
                    return true;
                }
                boolean scanWaiters = true;
                long waiter = jobArenaMgr.getLastWaiter(holderJobSlot);
                while (waiter >= 0) {
                    long watingOnResSlot = reqArenaMgr.getResourceId(waiter);
                    if (introducesDeadlock(watingOnResSlot, jobSlot, tracker)) {
                        return true;
                    }
                    waiter = reqArenaMgr.getNextJobRequest(waiter);
                    if (waiter < 0 && scanWaiters) {
                        scanWaiters = false;
                        waiter = jobArenaMgr.getLastUpgrader(holderJobSlot);
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
        
        final int dsId = datasetId.getId();        
        final int jobId = txnContext.getJobId().getId();

        if (entityHashValue != -1) {
            lock(datasetId, -1, LockMode.intentionMode(lockMode), txnContext);
        } else {
            throw new UnsupportedOperationException("instant locks are not supported on datasets");
        }

        final ResourceGroup group = table.get(dsId, entityHashValue);
        if (group.firstResourceIndex.get() == -1l) {
            validateJob(txnContext);
            // if we do not have a resource in the group, we know that the
            // resource that we are looking for is not locked 
            return;
        }

        // we only allocate a request slot if we actually have to wait
        long reqSlot = -1;

        group.getLatch();
        try {
            validateJob(txnContext);

            final long resSlot = findResourceInGroup(group, dsId, entityHashValue);
            if (resSlot < 0) {
                // if we don't find the resource, there are no locks on it.
                return;
            }

            final long jobSlot = findOrAllocJobSlot(jobId);

            while (true) {
                final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
                switch (act) {
                    case UPD:
                    case GET:
                        return;
                    case WAIT:
                    case CONV:
                        if (reqSlot == -1) {
                            reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);
                        }
                        enqueueWaiter(group, reqSlot, resSlot, jobSlot, act, txnContext);
                        break;
                    case ERR:
                    default:
                        throw new IllegalStateException();
                }
            }
        } finally {
            if (reqSlot != -1) {
                // deallocate request, if we allocated one earlier
                if (DEBUG_MODE) LOGGER.finer("del req slot " + TypeUtil.Global.toString(reqSlot));
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
        
        final int dsId = datasetId.getId();
        final int jobId = txnContext.getJobId().getId();

        if (entityHashValue != -1) {
            if (! tryLock(datasetId, -1, LockMode.intentionMode(lockMode), txnContext)) {
                return false;
            }
        } else {
            if (dsLockCache.get().contains(jobId, dsId, lockMode)) {
                return true;
            }
        }

        final long jobSlot = findOrAllocJobSlot(jobId);
        
        final ResourceGroup group = table.get(dsId, entityHashValue);
        group.getLatch();

        try {
            validateJob(txnContext);

            final long resSlot = findOrAllocResourceSlot(group, dsId, entityHashValue);
            final long reqSlot = allocRequestSlot(resSlot, jobSlot, lockMode);

            final LockAction act = determineLockAction(resSlot, jobSlot, lockMode);
            switch (act) {
                case UPD:
                    resArenaMgr.setMaxMode(resSlot, lockMode);
                    // no break
                case GET:
                    addHolder(reqSlot, resSlot, jobSlot);
                    if (entityHashValue == -1) {
                        dsLockCache.get().put(jobId, dsId, lockMode);
                    }
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

        // if we did acquire the dataset lock, but not the entity lock, we keep
        // it anyway and clean it up at the end of the job
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        log("instantTryLock", datasetId.getId(), entityHashValue, lockMode, txnContext);
        stats.instantTryLock();
        
        final int dsId = datasetId.getId();
        final int jobId = txnContext.getJobId().getId();

        if (entityHashValue != -1) {
            if (! tryLock(datasetId, -1, LockMode.intentionMode(lockMode), txnContext)) {
                return false;
            }
        } else {
            throw new UnsupportedOperationException("instant locks are not supported on datasets");
        }

        final ResourceGroup group = table.get(dsId, entityHashValue);
        if (group.firstResourceIndex.get() == -1l) {
            validateJob(txnContext);
            // if we do not have a resource in the group, we know that the
            // resource that we are looking for is not locked 
            return true;
        }

        group.getLatch();
        try {
            validateJob(txnContext);

            final long resSlot = findResourceInGroup(group, dsId, entityHashValue);
            if (resSlot < 0) {
                // if we don't find the resource, there are no locks on it.
                return true;
            }

            final long jobSlot = findOrAllocJobSlot(jobId);

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
        final int jobId = txnContext.getJobId().getId();
        final long jobSlot = jobIdSlotMap.get(jobId);
        final int dsId = datasetId.getId();
        unlock(dsId, entityHashValue, lockMode, jobSlot);
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
            
            if (CHECK_CONSISTENCY) assertLocksCanBefoundInJobQueue();
            
            long holder = removeLastHolder(resource, jobSlot, lockMode);

            // deallocate request
            if (DEBUG_MODE) LOGGER.finer("del req slot " + TypeUtil.Global.toString(holder));
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
                if (DEBUG_MODE) LOGGER.finer("del res slot " + TypeUtil.Global.toString(resource));
                resArenaMgr.deallocate(resource);
            } else {
                final int oldMaxMode = resArenaMgr.getMaxMode(resource);
                final int newMaxMode = determineNewMaxMode(resource, oldMaxMode);
                resArenaMgr.setMaxMode(resource, newMaxMode);
                if (oldMaxMode != newMaxMode) {
                    // the locking mode didn't change, current waiters won't be
                    // able to acquire the lock, so we do not need to signal them
                    group.wakeUp();
                }
            }
        } finally {
            group.releaseLatch();
        }

        // dataset intention locks are cleaned up at the end of the job
    }

    @Override
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException {
        log("releaseLocks", -1, -1, LockMode.ANY, txnContext);
        stats.releaseLocks();

        int jobId = txnContext.getJobId().getId();
        Long jobSlot = jobIdSlotMap.get(jobId);
        if (jobSlot == null) {
            // we don't know the job, so there are no locks for it - we're done
            return;
        }
        //System.err.println(table.append(new StringBuilder(), true).toString());
        if (LOGGER.isLoggable(LVL)) {
            LOGGER.log(LVL, "jobArenaMgr " + jobArenaMgr.addTo(new RecordManagerStats()).toString());
            LOGGER.log(LVL, "resArenaMgr " + resArenaMgr.addTo(new RecordManagerStats()).toString());
            LOGGER.log(LVL, "reqArenaMgr " + reqArenaMgr.addTo(new RecordManagerStats()).toString());
        }
        long holder;
        synchronized (jobArenaMgr) {
            holder = jobArenaMgr.getLastHolder(jobSlot);
        }
        while (holder != -1) {
            long resource = reqArenaMgr.getResourceId(holder);
            int dsId = resArenaMgr.getDatasetId(resource);
            int pkHashVal = resArenaMgr.getPkHashVal(resource);
            unlock(dsId, pkHashVal, LockMode.ANY, jobSlot);
            synchronized (jobArenaMgr) {
                holder = jobArenaMgr.getLastHolder(jobSlot);
            }
        }
        if (DEBUG_MODE) LOGGER.finer("del job slot " + TypeUtil.Global.toString(jobSlot));
        jobArenaMgr.deallocate(jobSlot);
        jobIdSlotMap.remove(jobId);
        stats.logCounters(LOGGER, Level.INFO, true);
    }

    private long findOrAllocJobSlot(int jobId) {
        Long jobSlot = jobIdSlotMap.get(jobId);
        if (jobSlot == null) {
            jobSlot = new Long(jobArenaMgr.allocate());
            if (DEBUG_MODE) LOGGER.finer("new job slot " + TypeUtil.Global.toString(jobSlot) + " (" + jobId + ")");
            jobArenaMgr.setJobId(jobSlot, jobId);
            Long oldSlot = jobIdSlotMap.putIfAbsent(jobId, jobSlot);
            if (oldSlot != null) {
                // if another thread allocated a slot for this jobId between
                // get(..) and putIfAbsent(..), we'll use that slot and
                // deallocate the one we allocated
                if (DEBUG_MODE) LOGGER.finer("del job slot " + TypeUtil.Global.toString(jobSlot) + " due to conflict");
                jobArenaMgr.deallocate(jobSlot);
                jobSlot = oldSlot;
            }
        }
        assert (jobSlot >= 0);
        return jobSlot;
    }

    private long findOrAllocResourceSlot(ResourceGroup group, int dsId, int entityHashValue) {
        long resSlot = findResourceInGroup(group, dsId, entityHashValue);

        if (resSlot == -1) {
            // we don't know about this resource, let's alloc a slot
            resSlot = resArenaMgr.allocate();
            resArenaMgr.setDatasetId(resSlot, dsId);
            resArenaMgr.setPkHashVal(resSlot, entityHashValue);
            resArenaMgr.setNext(resSlot, group.firstResourceIndex.get());
            group.firstResourceIndex.set(resSlot);
            if (DEBUG_MODE) LOGGER.finer("new res slot " + TypeUtil.Global.toString(resSlot) + " (" + dsId + ", " + entityHashValue + ")");
        } else {
            if (DEBUG_MODE) LOGGER.finer("fnd res slot " + TypeUtil.Global.toString(resSlot) + " (" + dsId + ", " + entityHashValue + ")");
        }
        return resSlot;
    }

    private long allocRequestSlot(long resSlot, long jobSlot, byte lockMode) {
        long reqSlot = reqArenaMgr.allocate();
        reqArenaMgr.setResourceId(reqSlot, resSlot);
        reqArenaMgr.setLockMode(reqSlot, lockMode); // lock mode is a byte!!
        reqArenaMgr.setJobSlot(reqSlot, jobSlot);
        if (DEBUG_MODE) {
            LOGGER.finer("new req slot " + TypeUtil.Global.toString(reqSlot)
                    + " (" + TypeUtil.Global.toString(resSlot)
                    + ", " + TypeUtil.Global.toString(jobSlot)
                    + ", " + LockMode.toString(lockMode) + ")");
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
        while (holder != -1) {
            if (job == reqArenaMgr.getJobSlot(holder)) {
                if (reqArenaMgr.getLockMode(holder) == lockMode) {
                    return LockAction.GET;
                } else {
                    res = LockAction.CONV;
                }
            }
            holder = reqArenaMgr.getNextRequest(holder);
        }
        return res;
    }

    private long findResourceInGroup(ResourceGroup group, int dsId, int entityHashValue) {
        stats.logCounters(LOGGER, Level.INFO, false);
        long resSlot = group.firstResourceIndex.get();
        while (resSlot != -1) {
            // either we already have a lock on this resource or we have a 
            // hash collision
            if (resArenaMgr.getDatasetId(resSlot) == dsId && resArenaMgr.getPkHashVal(resSlot) == entityHashValue) {
                return resSlot;
            } else {
                resSlot = resArenaMgr.getNext(resSlot);
            }
        }
        return -1;
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
        if (nextForJob != -1) {
            reqArenaMgr.setPrevJobRequest(nextForJob, prevForJob);
        }
        if (prevForJob == -1) {
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

    final Queue waiter = new Queue() {
        public void add(long request, long resource, long job) {
            long waiter = resArenaMgr.getFirstWaiter(resource);
            reqArenaMgr.setNextRequest(request, -1);
            if (waiter == -1) {
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

    final Queue upgrader = new Queue() {
        public void add(long request, long resource, long job) {
            long upgrader = resArenaMgr.getFirstUpgrader(resource);
            reqArenaMgr.setNextRequest(request, -1);
            if (upgrader == -1) {
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
        reqArenaMgr.setPrevJobRequest(newRequest, -1);
        if (oldRequest >= 0) {
            reqArenaMgr.setPrevJobRequest(oldRequest, newRequest);
        }
    }

    private void appendToRequestQueue(long head, long appendee) {
        long next = reqArenaMgr.getNextRequest(head);
        while (next != -1) {
            head = next;
            next = reqArenaMgr.getNextRequest(head);
        }
        reqArenaMgr.setNextRequest(head, appendee);
    }

    private long removeRequestFromQueueForSlot(long head, long reqSlot) {
        long cur = head;
        long prev = cur;
        while (prev != -1) {
            cur = reqArenaMgr.getNextRequest(prev);
            if (cur == -1) {
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
        while (prev != -1) {
            holder = reqArenaMgr.getNextRequest(prev);
            if (holder == -1) {
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
        while (holder != -1) {
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
        return resArenaMgr.getLastHolder(resource) == -1 && resArenaMgr.getFirstUpgrader(resource) == -1
                && resArenaMgr.getFirstWaiter(resource) == -1;
    }

    private void validateJob(ITransactionContext txnContext) throws ACIDException {
        if (txnContext.getTxnState() == ITransactionManager.ABORTED) {
            throw new ACIDException("" + txnContext.getJobId() + " is in ABORTED state.");
        } else if (txnContext.isTimeout()) {
            requestAbort(txnContext, "timeout");
        }
    }

    private void requestAbort(ITransactionContext txnContext, String msg) throws ACIDException {
        txnContext.setTimeout(true);
        throw new ACIDException("Transaction " + txnContext.getJobId()
                + " should abort (requested by the Lock Manager)" + ":\n" + msg);
    }

    /*
     * Debugging support
     */
    
    private void log(String string, int id, int entityHashValue, byte lockMode, ITransactionContext txnContext) {
        if (! LOGGER.isLoggable(LVL)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{ op : ").append(string);
        if (id != -1) {
            sb.append(" , dataset : ").append(id);
        }
        if (entityHashValue != -1) {
            sb.append(" , entity : ").append(entityHashValue);
        }
        if (lockMode != LockMode.NL) {
            sb.append(" , mode : ").append(LockMode.toString(lockMode));
        }
        if (txnContext != null) {
            sb.append(" , jobId : ").append(txnContext.getJobId());
        }
        sb.append(" , thread : ").append(Thread.currentThread().getName());
        sb.append(" }");
        LOGGER.log(LVL, sb.toString());
    }

    private void assertLocksCanBefoundInJobQueue() throws ACIDException {
        for (int i = 0; i < ResourceGroupTable.TABLE_SIZE; ++i) {
            final ResourceGroup group = table.get(i);
            if (group.tryLatch(100, TimeUnit.MILLISECONDS)) {
                try {
                    long resSlot = group.firstResourceIndex.get();
                    while (resSlot != -1) {
                        int dsId = resArenaMgr.getDatasetId(resSlot);
                        int entityHashValue = resArenaMgr.getPkHashVal(resSlot);
                        long reqSlot = resArenaMgr.getLastHolder(resSlot);
                        while (reqSlot != -1) {
                            byte lockMode = (byte) reqArenaMgr.getLockMode(reqSlot);
                            long jobSlot = reqArenaMgr.getJobSlot(reqSlot);
                            int jobId = jobArenaMgr.getJobId(jobSlot);
                            assertLockCanBeFoundInJobQueue(dsId, entityHashValue, lockMode, jobId);
                            reqSlot = reqArenaMgr.getNextRequest(reqSlot);
                        }
                        resSlot = resArenaMgr.getNext(resSlot);
                    }
                } finally {
                    group.releaseLatch();
                }
            } else {
                LOGGER.warning("Could not check locks for " + group);
            }
        }
    }
    
    private void assertLockCanBeFoundInJobQueue(int dsId, int entityHashValue, byte lockMode, int jobId) {
        if (findLockInJobQueue(dsId, entityHashValue, jobId, lockMode) == -1) {
            String msg = "request for " + LockMode.toString(lockMode) + " lock on dataset " + dsId + " entity "
                    + entityHashValue + " not found for job " + jobId + " in thread " + Thread.currentThread().getName();
            LOGGER.severe(msg);            
            throw new IllegalStateException(msg);
        }
    }

    /**
     * tries to find a lock request searching though the job queue
     * @param dsId dataset id
     * @param entityHashValue primary key hash value
     * @param jobId job id
     * @param lockMode lock mode
     * @return the slot of the request, if the lock request is found, -1 otherwise 
     */
    private long findLockInJobQueue(final int dsId, final int entityHashValue, final int jobId, byte lockMode) {
        Long jobSlot = jobIdSlotMap.get(jobId);
        if (jobSlot == null) {
            return -1;
        }

        long holder;
        synchronized (jobArenaMgr) {
            holder = jobArenaMgr.getLastHolder(jobSlot);
        }
        while (holder != -1) {
            long resource = reqArenaMgr.getResourceId(holder);
            if (dsId == resArenaMgr.getDatasetId(resource)
                    && entityHashValue == resArenaMgr.getPkHashVal(resource)
                    && jobSlot == reqArenaMgr.getJobSlot(holder)
                    && (lockMode == reqArenaMgr.getLockMode(holder)
                        || lockMode == LockMode.ANY)) {
                return holder;
            }
            synchronized (jobArenaMgr) {
                holder = reqArenaMgr.getNextJobRequest(holder);
            }
        }
        return -1;
    }

    private String resQueueToString(long resSlot) {
        return appendResQueue(new StringBuilder(), resSlot).toString();
    }
    
    private StringBuilder appendResQueue(StringBuilder sb, long resSlot) {
        resArenaMgr.appendRecord(sb, resSlot);
        sb.append("\n");
        appendReqQueue(sb, resArenaMgr.getLastHolder(resSlot));
        return sb;
    }
    
    private StringBuilder appendReqQueue(StringBuilder sb, long head) {
        while (head != -1) {
            reqArenaMgr.appendRecord(sb, head);
            sb.append("\n");
            head = reqArenaMgr.getNextRequest(head);
        }
        return sb;
    }
    
    public StringBuilder append(StringBuilder sb) {
        table.getAllLatches();
        try {
            sb.append(">>dump_begin\t>>----- [resTable] -----\n");
            table.append(sb);
            sb.append(">>dump_end\t>>----- [resTable] -----\n");

            sb.append(">>dump_begin\t>>----- [resArenaMgr] -----\n");
            resArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [resArenaMgr] -----\n");

            sb.append(">>dump_begin\t>>----- [reqArenaMgr] -----\n");
            reqArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [reqArenaMgr] -----\n");

            sb.append(">>dump_begin\t>>----- [jobIdSlotMap] -----\n");
            for (Integer i : jobIdSlotMap.keySet()) {
                sb.append(i).append(" : ");
                TypeUtil.Global.append(sb, jobIdSlotMap.get(i));
                sb.append("\n");
            }
            sb.append(">>dump_end\t>>----- [jobIdSlotMap] -----\n");

            sb.append(">>dump_begin\t>>----- [jobArenaMgr] -----\n");
            jobArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [jobArenaMgr] -----\n");
        } finally {
            table.releaseAllLatches();
        }
        return sb;
    }

    public String toString() {
        return append(new StringBuilder()).toString();
    }

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return append(s).toString() + "\n";
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        os.write(toString().getBytes());
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) throws IOException {
        if (dumpState) {
            dumpState(os);
        }
    }

    private static class DatasetLockCache {
        private long jobId = -1;
        private HashMap<Integer, Byte> lockCache = new HashMap<Integer, Byte>();
        // size 1 cache to avoid the boxing/unboxing that comes with the 
        // access to the HashMap
        private int cDsId = -1;
        private byte cDsLockMode = -1;

        public boolean contains(final int jobId, final int dsId, byte dsLockMode) {
            if (this.jobId == jobId) {
                if (this.cDsId == dsId && this.cDsLockMode == dsLockMode) {
                    return true;
                }
                final Byte cachedLockMode = this.lockCache.get(dsId);
                if (cachedLockMode != null && cachedLockMode == dsLockMode) {
                    this.cDsId = dsId;
                    this.cDsLockMode = dsLockMode;
                    return true;
                }
            } else {
                this.jobId = -1;
                this.cDsId = -1;
                this.cDsLockMode = -1;
                this.lockCache.clear();
            }
            return false;
        }

        public void put(final int jobId, final int dsId, byte dsLockMode) {
            this.jobId = jobId;
            this.cDsId = dsId;
            this.cDsLockMode = dsLockMode;
            this.lockCache.put(dsId, dsLockMode);
        }

        public String toString() {
            return "[ " + jobId + " : " + lockCache.toString() + "]";
        }
    }

    private static class ResourceGroupTable {
        public static final int TABLE_SIZE = 1024; // TODO increase?

        private ResourceGroup[] table;

        public ResourceGroupTable() {
            table = new ResourceGroup[TABLE_SIZE];
            for (int i = 0; i < TABLE_SIZE; ++i) {
                table[i] = new ResourceGroup();
            }
        }

        ResourceGroup get(int dId, int entityHashValue) {
            // TODO ensure good properties of hash function
            int h = Math.abs(dId ^ entityHashValue);
            if (h < 0) h = 0;
            return table[h % TABLE_SIZE];
        }
        
        ResourceGroup get(int i) {
            return table[i];
        }

        public void getAllLatches() {
            for (int i = 0; i < TABLE_SIZE; ++i) {
                table[i].getLatch();
            }
        }

        public void releaseAllLatches() {
            for (int i = 0; i < TABLE_SIZE; ++i) {
                table[i].releaseLatch();
            }
        }

        public StringBuilder append(StringBuilder sb) {
            return append(sb, false);
        }

        public StringBuilder append(StringBuilder sb, boolean detail) {
            for (int i = 0; i < table.length; ++i) {
                sb.append(i).append(" : ");
                if (detail) {
                    sb.append(table[i]);
                } else {
                    sb.append(table[i].firstResourceIndex);
                }
                sb.append('\n');
            }
            return sb;
        }
    }

    private static class ResourceGroup {
        private ReentrantReadWriteLock latch;
        private Condition condition;
        AtomicLong firstResourceIndex;

        ResourceGroup() {
            latch = new ReentrantReadWriteLock();
            condition = latch.writeLock().newCondition();
            firstResourceIndex = new AtomicLong(-1);
        }

        void getLatch() {
            log("latch");
            latch.writeLock().lock();
        }
        
        boolean tryLatch(long timeout, TimeUnit unit) throws ACIDException {
            log("tryLatch");
            try {
                return latch.writeLock().tryLock(timeout, unit);
            } catch (InterruptedException e) {
                LOGGER.finer("interrupted while wating on ResourceGroup");
                throw new ACIDException("interrupted", e);
            }
        }

        void releaseLatch() {
            log("release");
            latch.writeLock().unlock();
        }

        boolean hasWaiters() {
            return latch.hasQueuedThreads();
        }

        void await(ITransactionContext txnContext) throws ACIDException {
            log("wait for");
            try {
                condition.await();
            } catch (InterruptedException e) {
                LOGGER.finer("interrupted while wating on ResourceGroup");
                throw new ACIDException(txnContext, "interrupted", e);
            }
        }

        void wakeUp() {
            log("notify");
            condition.signalAll();
        }

        void log(String s) {
            if (LOGGER.isLoggable(LVL)) {
                LOGGER.log(LVL, s + " " + toString());
            }            
        }

        public String toString() {
            return "{ id : " + hashCode() + ", first : " + TypeUtil.Global.toString(firstResourceIndex.get()) + ", waiters : "
                    + (hasWaiters() ? "true" : "false") + " }";
        }
    }
}
