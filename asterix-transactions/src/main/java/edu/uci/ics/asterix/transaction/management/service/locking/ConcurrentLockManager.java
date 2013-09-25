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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the ILockManager interface for the
 * specific case of locking protocol with two lock modes: (S) and (X),
 * where S lock mode is shown by 0, and X lock mode is shown by 1.
 * 
 * @author tillw
 */

public class ConcurrentLockManager implements ILockManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true

    private TransactionSubsystem txnSubsystem;

    private ResourceGroupTable table;
    private ResourceArenaManager resArenaMgr;
    private RequestArenaManager reqArenaMgr;
    private ConcurrentHashMap<Integer, Integer> jobReqMap; // TODO different impl
        
    enum LockAction {
        GET,
        UPD, // special version of GET that updates the max lock mode
        WAIT
    }
    
    static LockAction[][] ACTION_MATRIX = {
        // new    NL              IS               IX                S                X
        { LockAction.GET,  LockAction.UPD,  LockAction.UPD,  LockAction.UPD,  LockAction.UPD  }, // NL
        { LockAction.WAIT, LockAction.GET,  LockAction.UPD,  LockAction.UPD,  LockAction.WAIT }, // IS
        { LockAction.WAIT, LockAction.GET,  LockAction.GET,  LockAction.WAIT, LockAction.WAIT }, // IX
        { LockAction.WAIT, LockAction.GET,  LockAction.WAIT, LockAction.GET,  LockAction.WAIT }, // S
        { LockAction.WAIT, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT, LockAction.WAIT }  // X
    };
        
    public ConcurrentLockManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;
        
        this.table = new ResourceGroupTable();
        
        final int lockManagerShrinkTimer = txnSubsystem.getTransactionProperties()
                .getLockManagerShrinkTimer();

        resArenaMgr = new ResourceArenaManager();
        reqArenaMgr = new RequestArenaManager();
        jobReqMap = new ConcurrentHashMap<>();
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return this.txnSubsystem.getTransactionProperties();
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        
        if (entityHashValue != -1) {
            // get the intention lock on the dataset, if we want to lock an individual item
            byte dsLockMode = lockMode == LockMode.X ? LockMode.IX : LockMode.IS;
            lock(datasetId, -1, dsLockMode, txnContext);
        }

        int dsId = datasetId.getId();

        ResourceGroup group = table.get(datasetId, entityHashValue);
        group.getLatch();

        // 1) Find the resource in the hash table
        
        int resSlot = findResourceInGroup(group, dsId, entityHashValue);
        
        if (resSlot == -1) {
            // we don't know about this resource, let's alloc a slot
            resSlot = resArenaMgr.allocate();
            resArenaMgr.setDatasetId(resSlot, datasetId.getId());
            resArenaMgr.setPkHashVal(resSlot, entityHashValue);

            if (group.firstResourceIndex.get() == -1) {
                group.firstResourceIndex.set(resSlot);
            }
        }
        
        // 2) create a request entry

        int jobId = txnContext.getJobId().getId();
        int reqSlot = reqArenaMgr.allocate();
        reqArenaMgr.setResourceId(reqSlot, resSlot);
        reqArenaMgr.setLockMode(reqSlot, lockMode); // lock mode is a byte!!
        reqArenaMgr.setJobId(reqSlot, jobId);
        
        int prevHead = -1;
        Integer headOfJobReqQueue = jobReqMap.putIfAbsent(jobId, reqSlot);
        while (headOfJobReqQueue != null) {
            // TODO make sure this works (even if the job gets removed from the table)
            if (jobReqMap.replace(jobId, headOfJobReqQueue, reqSlot)) {
                prevHead = headOfJobReqQueue;
                break;
            }
            headOfJobReqQueue = jobReqMap.putIfAbsent(jobId, reqSlot);
        }

        // this goes across arenas
        reqArenaMgr.setNextJobRequest(reqSlot, prevHead);
        reqArenaMgr.setPrevJobRequest(reqSlot, -1);
        if (prevHead >= 0) {
          reqArenaMgr.setPrevJobRequest(prevHead, reqSlot);
        }
        
        // 3) check lock compatibility
        
        boolean locked = false;
        
        while (! locked) {
            int curLockMode = resArenaMgr.getMaxMode(resSlot);
            switch (ACTION_MATRIX[curLockMode][lockMode]) {
                case UPD:
                    resArenaMgr.setMaxMode(resSlot, lockMode);
                    // no break
                case GET:
                    addHolderToResource(resSlot, reqSlot);
                    locked = true;
                    break;
                case WAIT:
                    // TODO can we have more than on upgrader? Or do we need to
                    // abort if we get a second upgrader?
                    if (findLastHolderForJob(resSlot, jobId) != -1) {
                        addUpgraderToResource(resSlot, reqSlot);
                    } else {
                        addWaiterToResource(resSlot, reqSlot);
                    }
                    try {
                        group.await();
                    } catch (InterruptedException e) {
                        throw new ACIDException(txnContext, "interrupted", e);
                    }
                    break;       
            }
            
            // TODO where do we check for deadlocks?
        }
        
        group.releaseLatch();
        
        //internalLock(datasetId, entityHashValue, lockMode, txnContext, false);
    }

    @Override
    public void unlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext) throws ACIDException {

        ResourceGroup group = table.get(datasetId, entityHashValue);
        group.getLatch();

        int dsId = datasetId.getId();
        int resource = findResourceInGroup(group, dsId, entityHashValue);
        
        if (resource < 0) {
            throw new IllegalStateException("resource (" + dsId + ",  " + entityHashValue + ") not found");
        }
        
        int jobId = txnContext.getJobId().getId();
        // since locking is properly nested, finding the last holder is good enough
        
        int holder = resArenaMgr.getLastHolder(resource);
        if (holder < 0) {
            throw new IllegalStateException("no holder for resource " + resource);
        }
        
        // remove from the list of holders
        if (reqArenaMgr.getJobId(holder) == jobId) {
            int next = reqArenaMgr.getNextRequest(holder);
            resArenaMgr.setLastHolder(resource, next);
        } else {
            int prev = holder;
            while (prev != -1) {
                holder = reqArenaMgr.getNextRequest(prev);
                if (holder == -1) {
                    throw new IllegalStateException("no holder for resource " + resource + " and job " + jobId);
                }
                if (jobId == reqArenaMgr.getJobId(holder)) {
                    break;
                }
                prev = holder;
            }
            int next = reqArenaMgr.getNextRequest(holder);
            reqArenaMgr.setNextRequest(prev, next);
        }
        
        // remove from the list of requests for a job
        int prevForJob = reqArenaMgr.getPrevJobRequest(holder);
        int nextForJob = reqArenaMgr.getNextJobRequest(holder);
        if (prevForJob == -1) {
            if (nextForJob == -1) {
                jobReqMap.remove(jobId);
            } else {
                jobReqMap.put(jobId, nextForJob);
            }
        } else {
            reqArenaMgr.setNextJobRequest(prevForJob, nextForJob);
        }
        if (nextForJob != -1) {
            reqArenaMgr.setPrevJobRequest(nextForJob, prevForJob);
        }
        
        // deallocate request
        reqArenaMgr.deallocate(holder);
        // deallocate resource or fix max lock mode
        if (resourceNotUsed(resource)) {
            int prev = group.firstResourceIndex.get();
            if (prev == resource) {
                group.firstResourceIndex.set(resArenaMgr.getNext(resource));
            } else {
                while (resArenaMgr.getNext(prev) != resource) {
                    prev = resArenaMgr.getNext(prev);
                }
                resArenaMgr.setNext(prev, resArenaMgr.getNext(resource));
            }
            resArenaMgr.deallocate(resource);
        } else {
            final int oldMaxMode = resArenaMgr.getMaxMode(resource);
            final int newMaxMode = getNewMaxMode(resource, oldMaxMode);
            resArenaMgr.setMaxMode(resource, newMaxMode);
            if (oldMaxMode != newMaxMode) {
                // the locking mode didn't change, current waiters won't be
                // able to acquire the lock, so we do not need to signal them
                group.wakeUp();
            }
        }
        group.releaseLatch();
        
        // finally remove the intention lock as well
        if (entityHashValue != -1) {
            // remove the intention lock on the dataset, if we want to lock an individual item
            unlock(datasetId, -1, txnContext);
        }
        
        //internalUnlock(datasetId, entityHashValue, txnContext, false, false);
    }
    
    @Override
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException {
        int jobId = txnContext.getJobId().getId();
        Integer head = jobReqMap.get(jobId);
        while (head != null) {
            int resource = reqArenaMgr.getResourceId(head);
            int dsId = resArenaMgr.getDatasetId(resource);
            int pkHashVal = resArenaMgr.getPkHashVal(resource);
            unlock(new DatasetId(dsId), pkHashVal, txnContext);
            head = jobReqMap.get(jobId);            
        }
    }
        
    private int findLastHolderForJob(int resource, int job) {
        int holder = resArenaMgr.getLastHolder(resource);
        while (holder != -1) {
            if (job == reqArenaMgr.getJobId(holder)) {
                return holder;
            }
            holder = reqArenaMgr.getNextRequest(holder);
        }
        return -1;
    }
    
    private int findResourceInGroup(ResourceGroup group, int dsId, int entityHashValue) {
        int resSlot = group.firstResourceIndex.get();
        while (resSlot != -1) {
            // either we already have a lock on this resource or we have a 
            // hash collision
            if (resArenaMgr.getDatasetId(resSlot) == dsId && 
                    resArenaMgr.getPkHashVal(resSlot) == entityHashValue) {
                return resSlot;
            } else {
                resSlot = resArenaMgr.getNext(resSlot);
            }
        }
        return -1;        
    }
    
    private void addHolderToResource(int resource, int request) {
        final int lastHolder = resArenaMgr.getLastHolder(resource);
        reqArenaMgr.setNextRequest(request, lastHolder);
        resArenaMgr.setLastHolder(resource, request);
    }

    private void addWaiterToResource(int resource, int request) {
        int waiter = resArenaMgr.getFirstWaiter(resource);
        reqArenaMgr.setNextRequest(request, -1);
        if (waiter == -1) {
            resArenaMgr.setFirstWaiter(resource, request);
        } else {
            appendToRequestQueue(waiter, request);
        }
    }

    private void addUpgraderToResource(int resource, int request) {
        int upgrader = resArenaMgr.getFirstUpgrader(resource);
        reqArenaMgr.setNextRequest(request, -1);
        if (upgrader == -1) {
            resArenaMgr.setFirstUpgrader(resource, request);
        } else {
            appendToRequestQueue(upgrader, request);
        }
    }

    private void appendToRequestQueue(int head, int appendee) {
        int next = reqArenaMgr.getNextRequest(head);
        while(next != -1) {
            head = next;
            next = reqArenaMgr.getNextRequest(head);
        }
        reqArenaMgr.setNextRequest(head, appendee);        
    }
    
    private int getNewMaxMode(int resource, int oldMaxMode) {
        int newMaxMode = LockMode.NL;
        int holder = resArenaMgr.getLastHolder(resource);
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
                    throw new IllegalStateException("incompatible locks in holder queue");
            }
        }
        return newMaxMode;        
    }
    
    private boolean resourceNotUsed(int resource) {
        return resArenaMgr.getLastHolder(resource) == -1
                && resArenaMgr.getFirstUpgrader(resource) == -1
                && resArenaMgr.getFirstWaiter(resource) == -1;
    }
    


    private void validateJob(ITransactionContext txnContext) throws ACIDException {
        if (txnContext.getTxnState() == ITransactionManager.ABORTED) {
            throw new ACIDException("" + txnContext.getJobId() + " is in ABORTED state.");
        } else if (txnContext.isTimeout()) {
            requestAbort(txnContext);
        }
    }

    //@Override
    public void unlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext, boolean commitFlag)
            throws ACIDException {
        throw new IllegalStateException();
        //internalUnlock(datasetId, entityHashValue, txnContext, false, commitFlag);
    }

    private void instantUnlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext)
            throws ACIDException {
        throw new IllegalStateException();
        //internalUnlock(datasetId, entityHashValue, txnContext, true, false);
    }

    @Override
    public void instantLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {

        //        try {
        //            internalLock(datasetId, entityHashValue, lockMode, txnContext);
        //            return;
        //        } finally {
        //            unlock(datasetId, entityHashValue, txnContext);
        //        }
        throw new IllegalStateException();
        //internalLock(datasetId, entityHashValue, lockMode, txnContext, true);
        //instantUnlock(datasetId, entityHashValue, txnContext);
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        throw new IllegalStateException();
        //return internalTryLock(datasetId, entityHashValue, lockMode, txnContext, false);
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        throw new IllegalStateException();
        //return internalInstantTryLock(datasetId, entityHashValue, lockMode, txnContext);
    }

    private void trackLockRequest(String msg, int requestType, DatasetId datasetIdObj, int entityHashValue,
            byte lockMode, ITransactionContext txnContext, DatasetLockInfo dLockInfo, int eLockInfo) {
/*
        StringBuilder s = new StringBuilder();
        LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, datasetIdObj,
                entityHashValue, lockMode, txnContext);
        s.append(Thread.currentThread().getId() + ":");
        s.append(msg);
        if (msg.equals("Granted")) {
            if (dLockInfo != null) {
                s.append("\t|D| ");
                s.append(dLockInfo.getIXCount()).append(",");
                s.append(dLockInfo.getISCount()).append(",");
                s.append(dLockInfo.getXCount()).append(",");
                s.append(dLockInfo.getSCount()).append(",");
                if (dLockInfo.getFirstUpgrader() != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
                s.append(",");
                if (dLockInfo.getFirstWaiter() != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
            }

            if (eLockInfo != -1) {
                s.append("\t|E| ");
                s.append(entityLockInfoManager.getXCount(eLockInfo)).append(",");
                s.append(entityLockInfoManager.getSCount(eLockInfo)).append(",");
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
                s.append(",");
                if (entityLockInfoManager.getFirstWaiter(eLockInfo) != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
            }
        }

        lockRequestTracker.addEvent(s.toString(), request);
        if (msg.equals("Requested")) {
            lockRequestTracker.addRequest(request);
        }
        System.out.println(request.prettyPrint() + "--> " + s.toString());
*/
    }

    public String getHistoryForAllJobs() {
/*        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getHistoryForAllJobs();
        }
*/
        return null;
    }

    public String getHistoryPerJob() {
/*        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getHistoryPerJob();
        }
*/
        return null;
    }

    public String getRequestHistoryForAllJobs() {
/*        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getRequestHistoryForAllJobs();
        }
*/
        return null;
    }

    private void requestAbort(ITransactionContext txnContext) throws ACIDException {
        txnContext.setTimeout(true);
        throw new ACIDException("Transaction " + txnContext.getJobId()
                + " should abort (requested by the Lock Manager)");
    }

    /**
     * For now, upgrading lock granule from entity-granule to dataset-granule is not supported!!
     * 
     * @param fromLockMode
     * @param toLockMode
     * @return
     */
    private boolean isLockUpgrade(byte fromLockMode, byte toLockMode) {
        return fromLockMode == LockMode.S && toLockMode == LockMode.X;
    }

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return s + "\n";
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        if (dumpState) {

            //#. dump Configurable Variables
            dumpConfVars(os);

            //#. dump jobHT
            dumpJobInfo(os);

            //#. dump datasetResourceHT
            dumpDatasetLockInfo(os);

            //#. dump entityLockInfoManager
            dumpEntityLockInfo(os);

            //#. dump entityInfoManager
            dumpEntityInfo(os);

            //#. dump lockWaiterManager

            dumpLockWaiterInfo(os);
            try {
                os.flush();
            } catch (IOException e) {
                //ignore
            }
        }
    }

    private void dumpConfVars(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            sb.append("\nESCALATE_TRHESHOLD_ENTITY_TO_DATASET: "
                    + txnSubsystem.getTransactionProperties().getEntityToDatasetLockEscalationThreshold());
//            sb.append("\nSHRINK_TIMER_THRESHOLD (entityLockInfoManager): "
//                    + entityLockInfoManager.getShrinkTimerThreshold());
//            sb.append("\nSHRINK_TIMER_THRESHOLD (entityInfoManager): " + entityInfoManager.getShrinkTimerThreshold());
//            sb.append("\nSHRINK_TIMER_THRESHOLD (lockWaiterManager): " + lockWaiterManager.getShrinkTimerThreshold());
            sb.append("\n>>dump_end\t>>----- [ConfVars] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    private void dumpJobInfo(OutputStream os) {
        JobId jobId;
        JobInfo jobInfo;
        StringBuilder sb = new StringBuilder();
/*
        try {
            sb.append("\n>>dump_begin\t>>----- [JobInfo] -----");
            Set<Map.Entry<JobId, JobInfo>> entrySet = jobHT.entrySet();
            if (entrySet != null) {
                for (Map.Entry<JobId, JobInfo> entry : entrySet) {
                    if (entry != null) {
                        jobId = entry.getKey();
                        if (jobId != null) {
                            sb.append("\n" + jobId);
                        } else {
                            sb.append("\nJID:null");
                        }

                        jobInfo = entry.getValue();
                        if (jobInfo != null) {
                            sb.append(jobInfo.coreDump());
                        } else {
                            sb.append("\nJobInfo:null");
                        }
                    }
                }
            }
            sb.append("\n>>dump_end\t>>----- [JobInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
*/
    }

    private void dumpDatasetLockInfo(OutputStream os) {
/*
        DatasetId datasetId;
        DatasetLockInfo datasetLockInfo;
        StringBuilder sb = new StringBuilder();

        try {
            sb.append("\n>>dump_begin\t>>----- [DatasetLockInfo] -----");
            Set<Map.Entry<DatasetId, DatasetLockInfo>> entrySet = datasetResourceHT.entrySet();
            if (entrySet != null) {
                for (Map.Entry<DatasetId, DatasetLockInfo> entry : entrySet) {
                    if (entry != null) {
                        datasetId = entry.getKey();
                        if (datasetId != null) {
                            sb.append("\nDatasetId:" + datasetId.getId());
                        } else {
                            sb.append("\nDatasetId:null");
                        }

                        datasetLockInfo = entry.getValue();
                        if (datasetLockInfo != null) {
                            sb.append(datasetLockInfo.coreDump());
                        } else {
                            sb.append("\nDatasetLockInfo:null");
                        }
                    }
                    sb.append("\n>>dump_end\t>>----- [DatasetLockInfo] -----\n");
                    os.write(sb.toString().getBytes());

                    //create a new sb to avoid possible OOM exception
                    sb = new StringBuilder();
                }
            }
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
*/
    }

    private void dumpEntityLockInfo(OutputStream os) {
/*
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("\n>>dump_begin\t>>----- [EntityLockInfo] -----");
            entityLockInfoManager.coreDump(os);
            sb.append("\n>>dump_end\t>>----- [EntityLockInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
*/
    }

    private void dumpEntityInfo(OutputStream os) {
/*
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("\n>>dump_begin\t>>----- [EntityInfo] -----");
            entityInfoManager.coreDump(os);
            sb.append("\n>>dump_end\t>>----- [EntityInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
*/
    }

    private void dumpLockWaiterInfo(OutputStream os) {
/*
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("\n>>dump_begin\t>>----- [LockWaiterInfo] -----");
            lockWaiterManager.coreDump(os);
            sb.append("\n>>dump_end\t>>----- [LockWaiterInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
*/
    }

    private static class ResourceGroupTable {
        public static final int TABLE_SIZE = 10; // TODO increase

        private ResourceGroup[] table;
        
        public ResourceGroupTable() {
            table = new ResourceGroup[TABLE_SIZE];
            for (int i = 0; i < TABLE_SIZE; ++i) {
                table[i] = new ResourceGroup();
            }
        }
        
        ResourceGroup get(DatasetId dId, int entityHashValue) {
            // TODO ensure good properties of hash function
            int h = Math.abs(dId.getId() ^ entityHashValue);
            return table[h % TABLE_SIZE];
        }
    }
    
    private static class ResourceGroup {
        private ReentrantReadWriteLock latch;
        private Condition condition;
        AtomicInteger firstResourceIndex;

        ResourceGroup() {
            latch = new ReentrantReadWriteLock();
            condition = latch.writeLock().newCondition();
            firstResourceIndex = new AtomicInteger(-1);
        }
        
        void getLatch() {
            latch.writeLock().lock();
        }
        
        void releaseLatch() {
            latch.writeLock().unlock();
        }
        
        boolean hasWaiters() {
            return latch.hasQueuedThreads();
        }
        
        void await() throws InterruptedException {
            condition.await();
        }
        
        void wakeUp() {
            condition.signalAll();
        }
    }
}
