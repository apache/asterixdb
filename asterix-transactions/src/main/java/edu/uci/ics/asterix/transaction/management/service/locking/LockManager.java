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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.logging.LogPage;
import edu.uci.ics.asterix.transaction.management.service.logging.LogPageReader;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecord;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the ILockManager interface for the
 * specific case of locking protocol with two lock modes: (S) and (X),
 * where S lock mode is shown by 0, and X lock mode is shown by 1.
 * 
 * @author pouria, kisskys
 */

public class LockManager implements ILockManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    //This variable indicates that the dataset granule X lock request is allowed when 
    //there are concurrent lock requests. As of 4/16/2013, we only allow the dataset granule X lock 
    //during DDL operation which is preceded by holding X latch on metadata.
    //Therefore, we don't allow the concurrent lock requests with the dataset granule X lock. 
    public static final boolean ALLOW_DATASET_GRANULE_X_LOCK_WITH_OTHER_CONCURRENT_LOCK_REQUESTS = false;

    public static final boolean ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET = true;
    private static final int DO_ESCALATE = 0;
    private static final int ESCALATED = 1;
    private static final int DONOT_ESCALATE = 2;

    private TransactionSubsystem txnSubsystem;

    //all threads accessing to LockManager's tables such as jobHT and datasetResourceHT
    //are serialized through LockTableLatch. All threads waiting the latch will be fairly served
    //in FIFO manner when the latch is available. 
    private final ReadWriteLock lockTableLatch;
    private final ReadWriteLock waiterLatch;
    private HashMap<JobId, JobInfo> jobHT;
    private HashMap<DatasetId, DatasetLockInfo> datasetResourceHT;

    private EntityLockInfoManager entityLockInfoManager;
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;

    private DeadlockDetector deadlockDetector;
    private TimeOutDetector toutDetector;
    private DatasetId tempDatasetIdObj; //temporary object to avoid object creation
    private JobId tempJobIdObj;

    private int tryLockDatasetGranuleRevertOperation;

    private LockRequestTracker lockRequestTracker; //for debugging
    private ConsecutiveWakeupContext consecutiveWakeupContext;

    public LockManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;
        this.lockTableLatch = new ReentrantReadWriteLock(true);
        this.waiterLatch = new ReentrantReadWriteLock(true);
        this.jobHT = new HashMap<JobId, JobInfo>();
        this.datasetResourceHT = new HashMap<DatasetId, DatasetLockInfo>();
        this.entityInfoManager = new EntityInfoManager(txnSubsystem.getTransactionProperties()
                .getLockManagerShrinkTimer());
        this.lockWaiterManager = new LockWaiterManager();
        this.entityLockInfoManager = new EntityLockInfoManager(entityInfoManager, lockWaiterManager);
        this.deadlockDetector = new DeadlockDetector(jobHT, datasetResourceHT, entityLockInfoManager,
                entityInfoManager, lockWaiterManager);
        this.toutDetector = new TimeOutDetector(this);
        this.tempDatasetIdObj = new DatasetId(0);
        this.tempJobIdObj = new JobId(0);
        this.consecutiveWakeupContext = new ConsecutiveWakeupContext();
        if (IS_DEBUG_MODE) {
            this.lockRequestTracker = new LockRequestTracker();
        }
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return this.txnSubsystem.getTransactionProperties();
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        internalLock(datasetId, entityHashValue, lockMode, txnContext, false);
    }

    private void internalLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext,
            boolean isInstant) throws ACIDException {

        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int entityInfo;
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
        boolean doEscalate = false;
        boolean caughtLockMgrLatchException = false;

        latchLockTable();
        try {
            validateJob(txnContext);

            if (IS_DEBUG_MODE) {
                trackLockRequest("Requested", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext,
                        dLockInfo, eLockInfo);
            }

            dLockInfo = datasetResourceHT.get(datasetId);
            jobInfo = jobHT.get(jobId);

            if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                if (!isInstant && datasetLockMode == LockMode.IS && jobInfo != null && dLockInfo != null) {
                    int escalateStatus = needEscalateFromEntityToDataset(jobInfo, dId, lockMode);
                    switch (escalateStatus) {
                        case DO_ESCALATE:
                            entityHashValue = -1;
                            doEscalate = true;
                            break;

                        case ESCALATED:
                            return;

                        default:
                            break;
                    }
                }
            }

            //#. if the datasetLockInfo doesn't exist in datasetResourceHT 
            if (dLockInfo == null || dLockInfo.isNoHolder()) {
                if (dLockInfo == null) {
                    dLockInfo = new DatasetLockInfo(entityLockInfoManager, entityInfoManager, lockWaiterManager);
                    datasetResourceHT.put(new DatasetId(dId), dLockInfo); //datsetId obj should be created
                }
                entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);

                //if dataset-granule lock
                if (entityHashValue == -1) { //-1 stands for dataset-granule
                    entityInfoManager.increaseDatasetLockCount(entityInfo);
                    dLockInfo.increaseLockCount(datasetLockMode);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    entityInfoManager.increaseDatasetLockCount(entityInfo);
                    dLockInfo.increaseLockCount(datasetLockMode);
                    //add entityLockInfo
                    eLockInfo = entityLockInfoManager.allocate();
                    dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }

                if (jobInfo == null) {
                    jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                    jobHT.put(jobId, jobInfo); //jobId obj doesn't have to be created
                }
                jobInfo.addHoldingResource(entityInfo);

                if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                    if (!isInstant && datasetLockMode == LockMode.IS) {
                        jobInfo.increaseDatasetISLockCount(dId);
                        if (doEscalate) {
                            throw new IllegalStateException(
                                    "ESCALATE_TRHESHOLD_ENTITY_TO_DATASET should not be set to "
                                            + txnSubsystem.getTransactionProperties()
                                                    .getEntityToDatasetLockEscalationThreshold());
                        }
                    }
                }

                if (IS_DEBUG_MODE) {
                    trackLockRequest("Granted", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext,
                            dLockInfo, eLockInfo);
                }

                return;
            }

            //#. the datasetLockInfo exists in datasetResourceHT.
            //1. handle dataset-granule lock
            entityInfo = lockDatasetGranule(datasetId, entityHashValue, lockMode, txnContext);

            //2. handle entity-granule lock
            if (entityHashValue != -1) {
                lockEntityGranule(datasetId, entityHashValue, lockMode, entityInfo, txnContext);
            }

            if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                if (!isInstant) {
                    if (doEscalate) {
                        //jobInfo must not be null.
                        assert jobInfo != null;
                        jobInfo.increaseDatasetISLockCount(dId);
                        //release pre-acquired locks
                        releaseDatasetISLocks(jobInfo, jobId, datasetId, txnContext);
                    } else if (datasetLockMode == LockMode.IS) {
                        if (jobInfo == null) {
                            jobInfo = jobHT.get(jobId);
                            //jobInfo must not be null;
                            assert jobInfo != null;
                        }
                        jobInfo.increaseDatasetISLockCount(dId);
                    }
                }
            }

            if (IS_DEBUG_MODE) {
                trackLockRequest("Granted", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext,
                        dLockInfo, eLockInfo);
            }
        } catch (Exception e) {
            if (e instanceof LockMgrLatchHandlerException) {
                // don't unlatch
                caughtLockMgrLatchException = true;
                throw new ACIDException(((LockMgrLatchHandlerException) e).getInternalException());
            }
        } finally {
            if (!caughtLockMgrLatchException) {
                unlatchLockTable();
            }
        }

        return;
    }

    private void releaseDatasetISLocks(JobInfo jobInfo, JobId jobId, DatasetId datasetId, ITransactionContext txnContext)
            throws ACIDException {
        int entityInfo;
        int prevEntityInfo;
        int entityHashValue;
        int did;//int-type dataset id

        //while traversing all holding resources, 
        //release IS locks on the escalated dataset and
        //release S locks on the corresponding enttites
        //by calling unlock() method.
        entityInfo = jobInfo.getLastHoldingResource();
        while (entityInfo != -1) {
            prevEntityInfo = entityInfoManager.getPrevJobResource(entityInfo);

            //release a lock only if the datset is the escalated dataset and
            //the entityHashValue is not -1("not -1" means a non-dataset-level lock)
            did = entityInfoManager.getDatasetId(entityInfo);
            entityHashValue = entityInfoManager.getPKHashVal(entityInfo);
            if (did == datasetId.getId() && entityHashValue != -1) {
                this.unlock(datasetId, entityHashValue, txnContext);
            }

            entityInfo = prevEntityInfo;
        }
    }

    private int needEscalateFromEntityToDataset(JobInfo jobInfo, int datasetId, byte lockMode) {
        //we currently allow upgrade only if the lockMode is S. 
        if (lockMode != LockMode.S) {
            return DONOT_ESCALATE;
        }

        int count = jobInfo.getDatasetISLockCount(datasetId);
        if (count == txnSubsystem.getTransactionProperties().getEntityToDatasetLockEscalationThreshold()) {
            return DO_ESCALATE;
        } else if (count > txnSubsystem.getTransactionProperties().getEntityToDatasetLockEscalationThreshold()) {
            return ESCALATED;
        } else {
            return DONOT_ESCALATE;
        }
    }

    private void validateJob(ITransactionContext txnContext) throws ACIDException {
        if (txnContext.getTxnState() == ITransactionManager.ABORTED) {
            throw new ACIDException("" + txnContext.getJobId() + " is in ABORTED state.");
        } else if (txnContext.isTimeout()) {
            requestAbort(txnContext);
        }
    }

    private int lockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int waiterObjId;
        int entityInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int weakerModeLockCount;
        int waiterCount = 0;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //check duplicated call

        //1. lock request causing duplicated upgrading requests from different threads in a same job
        waiterObjId = dLockInfo.findUpgraderFromUpgraderList(jId, entityHashValue);
        if (waiterObjId != -1) {
            //make the caller wait on the same LockWaiter object
            entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
            waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, true, true, txnContext, jobInfo, waiterObjId);

            //Only for the first-get-up thread, the waiterCount will be more than 0 and
            //the thread updates lock count on behalf of the all other waiting threads.
            //Therefore, all the next-get-up threads will not update any lock count.
            if (waiterCount > 0) {
                //add ((the number of waiting upgrader) - 1) to entityInfo's dataset lock count and datasetLockInfo's lock count
                //where -1 is for not counting the first upgrader's request since the lock count for the first upgrader's request
                //is already counted.
                weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                entityInfoManager.setDatasetLockMode(entityInfo, lockMode);
                entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount - 1);

                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount + waiterCount - 1);//new lock mode
                    dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                } else {
                    dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount + waiterCount - 1);
                    dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                }
            }

            return entityInfo;
        }

        //2. lock request causing duplicated waiting requests from different threads in a same job
        waiterObjId = dLockInfo.findWaiterFromWaiterList(jId, entityHashValue);
        if (waiterObjId != -1) {
            //make the caller wait on the same LockWaiter object
            entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
            waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, false, true, txnContext, jobInfo, waiterObjId);

            if (waiterCount > 0) {
                entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount);
                if (entityHashValue == -1) {
                    dLockInfo.increaseLockCount(datasetLockMode, waiterCount);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    dLockInfo.increaseLockCount(datasetLockMode, waiterCount);
                    //IS and IX holders are implicitly handled.
                }
                //add entityInfo to JobInfo's holding-resource list
                jobInfo.addHoldingResource(entityInfo);
            }

            return entityInfo;
        }

        //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
        entityInfo = dLockInfo.findEntityInfoFromHolderList(jId, entityHashValue);
        if (entityInfo == -1) {

            entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);
            if (jobInfo == null) {
                jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                jobHT.put(jobId, jobInfo);
            }

            //wait if any upgrader exists or upgrading lock mode is not compatible
            if (dLockInfo.getFirstUpgrader() != -1 || dLockInfo.getFirstWaiter() != -1
                    || !dLockInfo.isCompatible(datasetLockMode)) {

                /////////////////////////////////////////////////////////////////////////////////////////////
                //[Notice] Mimicking SIX mode
                //When the lock escalation from IS to S in dataset-level is allowed, the following case occurs
                //DatasetLockInfo's SCount = 1 and the same job who carried out the escalation tries to insert,
                //then the job should be able to insert without being blocked by itself. 
                //Our approach is to introduce SIX mode, but we don't have currently, 
                //so I simply mimicking SIX by allowing S and IX coexist in the dataset level 
                //only if their job id is identical for the requests. 
                if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                    if (datasetLockMode == LockMode.IX && dLockInfo.getSCount() == 1
                            && jobInfo.isDatasetLockGranted(dId, LockMode.S)) {
                        entityInfoManager.increaseDatasetLockCount(entityInfo);
                        //IX holders are implicitly handled without adding holder
                        dLockInfo.increaseLockCount(datasetLockMode);
                        //add entityInfo to JobInfo's holding-resource list
                        jobInfo.addHoldingResource(entityInfo);
                        return entityInfo;
                    }
                }
                ///////////////////////////////////////////////////////////////////////////////////////////////

                /////////////////////////////////////////////////////////////////////////////////////////////
                if (ALLOW_DATASET_GRANULE_X_LOCK_WITH_OTHER_CONCURRENT_LOCK_REQUESTS) {
                    //The following case only may occur when the dataset level X lock is requested 
                    //with the other lock

                    //[Notice]
                    //There has been no same caller as (jId, dId, entityHashValue) triplet.
                    //But there could be the same caller in terms of (jId, dId) pair.
                    //For example, 
                    //1) (J1, D1, E1) acquires IS in Dataset D1
                    //2) (J2, D1, -1) requests X  in Dataset D1, but waits
                    //3) (J1, D1, E2) requests IS in Dataset D1, but should wait 
                    //The 3) may cause deadlock if 1) and 3) are under the same thread.
                    //Even if (J1, D1, E1) and (J1, D1, E2) are two different thread, instead of
                    //aborting (J1, D1, E1) triggered by the deadlock, we give higher priority to 3) than 2)
                    //as long as the dataset level lock D1 is being held by the same jobId. 
                    //The above consideration is covered in the following code.
                    //find the same dataset-granule lock request, that is, (J1, D1) pair in the above example.
                    if (jobInfo.isDatasetLockGranted(dId, LockMode.IS)) {
                        if (dLockInfo.isCompatible(datasetLockMode)) {
                            //this is duplicated call
                            entityInfoManager.increaseDatasetLockCount(entityInfo);
                            if (entityHashValue == -1) {
                                dLockInfo.increaseLockCount(datasetLockMode);
                                dLockInfo.addHolder(entityInfo);
                            } else {
                                dLockInfo.increaseLockCount(datasetLockMode);
                                //IS and IX holders are implicitly handled.
                            }
                            //add entityInfo to JobInfo's holding-resource list
                            jobInfo.addHoldingResource(entityInfo);

                            return entityInfo;
                        } else {
                            //considered as upgrader
                            waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, true, true, txnContext, jobInfo,
                                    -1);
                            if (waiterCount > 0) {
                                entityInfoManager.increaseDatasetLockCount(entityInfo);
                                if (entityHashValue == -1) {
                                    dLockInfo.increaseLockCount(datasetLockMode);
                                    dLockInfo.addHolder(entityInfo);
                                } else {
                                    dLockInfo.increaseLockCount(datasetLockMode);
                                    //IS and IX holders are implicitly handled.
                                }
                                //add entityInfo to JobInfo's holding-resource list
                                jobInfo.addHoldingResource(entityInfo);
                            }
                            return entityInfo;
                        }
                    }
                }
                /////////////////////////////////////////////////////////////////////////////////////////////

                waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, false, true, txnContext, jobInfo, -1);
            } else {
                waiterCount = 1;
            }

            if (waiterCount > 0) {
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                if (entityHashValue == -1) {
                    dLockInfo.increaseLockCount(datasetLockMode);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    dLockInfo.increaseLockCount(datasetLockMode);
                    //IS and IX holders are implicitly handled.
                }
                //add entityInfo to JobInfo's holding-resource list
                jobInfo.addHoldingResource(entityInfo);
            }
        } else {
            isUpgrade = isLockUpgrade(entityInfoManager.getDatasetLockMode(entityInfo), lockMode);
            if (isUpgrade) { //upgrade call 
                //wait if any upgrader exists or upgrading lock mode is not compatible
                if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                    waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, true, true, txnContext, jobInfo, -1);
                } else {
                    waiterCount = 1;
                }

                if (waiterCount > 0) {
                    //add ((the number of waiting upgrader) - 1) to entityInfo's dataset lock count and datasetLockInfo's lock count
                    //where -1 is for not counting the first upgrader's request since the lock count for the first upgrader's request
                    //is already counted.
                    weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                    entityInfoManager.setDatasetLockMode(entityInfo, lockMode);
                    entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount - 1);

                    if (entityHashValue == -1) { //dataset-granule lock
                        dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount + waiterCount - 1);//new lock mode
                        dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                    } else {
                        dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount + waiterCount - 1);
                        dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                    }
                }
            } else { //duplicated call
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                datasetLockMode = entityInfoManager.getDatasetLockMode(entityInfo);

                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                } else { //entity-granule
                    datasetLockMode = datasetLockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                    dLockInfo.increaseLockCount(datasetLockMode);
                }
            }
        }

        return entityInfo;
    }

    private void lockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfoFromDLockInfo, ITransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int waiterObjId;
        int eLockInfo = -1;
        int entityInfo;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int waiterCount = 0;
        int weakerModeLockCount;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

        if (eLockInfo != -1) {
            //check duplicated call

            //1. lock request causing duplicated upgrading requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findUpgraderFromUpgraderList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
                waiterCount = handleLockWaiter(dLockInfo, eLockInfo, -1, true, false, txnContext, jobInfo, waiterObjId);

                if (waiterCount > 0) {
                    weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                    entityInfoManager.setEntityLockMode(entityInfo, LockMode.X);
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount - 1);

                    entityLockInfoManager.increaseLockCount(eLockInfo, LockMode.X, (short) (weakerModeLockCount
                            + waiterCount - 1));//new lock mode
                    entityLockInfoManager.decreaseLockCount(eLockInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode 
                }
                return;
            }

            //2. lock request causing duplicated waiting requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
                waiterCount = handleLockWaiter(dLockInfo, eLockInfo, -1, false, false, txnContext, jobInfo, waiterObjId);

                if (waiterCount > 0) {
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode, (short) waiterCount);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }
                return;
            }

            //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jId, entityHashValue);
            if (entityInfo != -1) {//duplicated call or upgrader

                isUpgrade = isLockUpgrade(entityInfoManager.getEntityLockMode(entityInfo), lockMode);
                if (isUpgrade) {//upgrade call
                    //wait if any upgrader exists or upgrading lock mode is not compatible
                    if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                            || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                        waiterCount = handleLockWaiter(dLockInfo, eLockInfo, entityInfo, true, false, txnContext,
                                jobInfo, -1);
                    } else {
                        waiterCount = 1;
                    }

                    if (waiterCount > 0) {
                        weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                        entityInfoManager.setEntityLockMode(entityInfo, lockMode);
                        entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount - 1);

                        entityLockInfoManager.increaseLockCount(eLockInfo, LockMode.X, (short) (weakerModeLockCount
                                + waiterCount - 1));//new lock mode
                        entityLockInfoManager.decreaseLockCount(eLockInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode 
                    }

                } else {//duplicated call
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, entityInfoManager.getEntityLockMode(entityInfo));
                }
            } else {//new call from this job, but still eLockInfo exists since other threads hold it or wait on it
                entityInfo = entityInfoFromDLockInfo;
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                        || entityLockInfoManager.getFirstWaiter(eLockInfo) != -1
                        || !entityLockInfoManager.isCompatible(eLockInfo, lockMode)) {
                    waiterCount = handleLockWaiter(dLockInfo, eLockInfo, entityInfo, false, false, txnContext, jobInfo,
                            -1);
                } else {
                    waiterCount = 1;
                }

                if (waiterCount > 0) {
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode, (short) waiterCount);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }
            }
        } else {//eLockInfo doesn't exist, so this lock request is the first request and can be granted without waiting.
            eLockInfo = entityLockInfoManager.allocate();
            dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
            entityInfoManager.increaseEntityLockCount(entityInfoFromDLockInfo);
            entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
            entityLockInfoManager.addHolder(eLockInfo, entityInfoFromDLockInfo);
        }
    }

    @Override
    public void unlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext) throws ACIDException {
        internalUnlock(datasetId, entityHashValue, txnContext, false);
    }

    private void instantUnlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext)
            throws ACIDException {
        internalUnlock(datasetId, entityHashValue, txnContext, true);
    }

    private void internalUnlock(DatasetId datasetId, int entityHashValue, ITransactionContext txnContext,
            boolean isInstant) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        int entityInfo = -1;
        byte datasetLockMode;

        if (IS_DEBUG_MODE) {
            if (entityHashValue == -1) {
                throw new UnsupportedOperationException(
                        "Unsupported unlock request: dataset-granule unlock is not supported");
            }
        }

        latchLockTable();
        try {
            if (IS_DEBUG_MODE) {
                trackLockRequest("Requested", RequestType.UNLOCK, datasetId, entityHashValue, (byte) 0, txnContext,
                        dLockInfo, eLockInfo);
            }

            //find the resource to be unlocked
            dLockInfo = datasetResourceHT.get(datasetId);
            jobInfo = jobHT.get(jobId);
            if (dLockInfo == null || jobInfo == null) {
                throw new IllegalStateException("Invalid unlock request: Corresponding lock info doesn't exist.");
            }

            eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

            if (eLockInfo == -1) {
                throw new IllegalStateException("Invalid unlock request: Corresponding lock info doesn't exist.");
            }

            //find the corresponding entityInfo
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jobId.getId(), entityHashValue);
            if (entityInfo == -1) {
                throw new IllegalStateException("Invalid unlock request[" + jobId.getId() + "," + datasetId.getId()
                        + "," + entityHashValue + "]: Corresponding lock info doesn't exist.");
            }

            datasetLockMode = entityInfoManager.getDatasetLockMode(entityInfo) == LockMode.S ? LockMode.IS
                    : LockMode.IX;

            //decrease the corresponding count of dLockInfo/eLockInfo/entityInfo
            dLockInfo.decreaseLockCount(datasetLockMode);
            entityLockInfoManager.decreaseLockCount(eLockInfo, entityInfoManager.getEntityLockMode(entityInfo));
            entityInfoManager.decreaseDatasetLockCount(entityInfo);
            entityInfoManager.decreaseEntityLockCount(entityInfo);

            if (entityInfoManager.getEntityLockCount(entityInfo) == 0
                    && entityInfoManager.getDatasetLockCount(entityInfo) == 0) {
                int threadCount = 0; //number of threads(in the same job) waiting on the same resource 
                int waiterObjId = jobInfo.getFirstWaitingResource();
                int waitingEntityInfo;
                LockWaiter waiterObj;

                //1) wake up waiters and remove holder
                //wake up waiters of dataset-granule lock
                wakeUpDatasetLockWaiters(dLockInfo);
                //wake up waiters of entity-granule lock
                wakeUpEntityLockWaiters(eLockInfo);
                //remove the holder from eLockInfo's holder list and remove the holding resource from jobInfo's holding resource list
                //this can be done in the following single function call.
                entityLockInfoManager.removeHolder(eLockInfo, entityInfo, jobInfo);

                //2) if 
                //      there is no waiting thread on the same resource (this can be checked through jobInfo)
                //   then 
                //      a) delete the corresponding entityInfo
                //      b) write commit log for the unlocked resource(which is a committed txn).
                while (waiterObjId != -1) {
                    waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                    waitingEntityInfo = waiterObj.getEntityInfoSlot();
                    if (entityInfoManager.getDatasetId(waitingEntityInfo) == datasetId.getId()
                            && entityInfoManager.getPKHashVal(waitingEntityInfo) == entityHashValue) {
                        threadCount++;
                        break;
                    }
                    waiterObjId = waiterObj.getNextWaiterObjId();
                }
                if (threadCount == 0) {
                    entityInfoManager.deallocate(entityInfo);
                }
            }

            //deallocate entityLockInfo's slot if there is no txn referring to the entityLockInfo.
            if (entityLockInfoManager.getFirstWaiter(eLockInfo) == -1
                    && entityLockInfoManager.getLastHolder(eLockInfo) == -1
                    && entityLockInfoManager.getUpgrader(eLockInfo) == -1) {
                dLockInfo.getEntityResourceHT().remove(entityHashValue);
                entityLockInfoManager.deallocate(eLockInfo);
            }

            //we don't deallocate datasetLockInfo even if there is no txn referring to the datasetLockInfo
            //since the datasetLockInfo is likely to be referred to again.

            if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                if (!isInstant && datasetLockMode == LockMode.IS) {
                    jobInfo.decreaseDatasetISLockCount(datasetId.getId(), txnSubsystem.getTransactionProperties()
                            .getEntityToDatasetLockEscalationThreshold());
                }
            }

            if (IS_DEBUG_MODE) {
                trackLockRequest("Granted", RequestType.UNLOCK, datasetId, entityHashValue, (byte) 0, txnContext,
                        dLockInfo, eLockInfo);
            }
        } finally {
            unlatchLockTable();
        }
    }

    @Override
    public void releaseLocks(ITransactionContext txnContext) throws ACIDException {
        LockWaiter waiterObj;
        int entityInfo;
        int prevEntityInfo;
        int entityHashValue;
        DatasetLockInfo dLockInfo = null;
        int eLockInfo = -1;
        int did;//int-type dataset id
        int datasetLockCount;
        int entityLockCount;
        byte lockMode;
        boolean existWaiter = false;

        JobId jobId = txnContext.getJobId();

        latchLockTable();
        try {
            if (IS_DEBUG_MODE) {
                trackLockRequest("Requested", RequestType.RELEASE_LOCKS, new DatasetId(0), 0, (byte) 0, txnContext,
                        dLockInfo, eLockInfo);
            }

            JobInfo jobInfo = jobHT.get(jobId);
            if (jobInfo == null) {
                return;
            }

            //remove waiterObj of JobInfo 
            //[Notice]
            //waiterObjs may exist if aborted thread is the caller of this function.
            //Even if there are the waiterObjs, there is no waiting thread on the objects. 
            //If the caller of this function is an aborted thread, it is guaranteed that there is no waiting threads
            //on the waiterObjs since when the aborted caller thread is waken up, all other waiting threads are
            //also waken up at the same time through 'notifyAll()' call.
            //In contrast, if the caller of this function is not an aborted thread, then there is no waiting object.
            int waiterObjId = jobInfo.getFirstWaitingResource();
            int nextWaiterObjId;
            while (waiterObjId != -1) {
                existWaiter = true;
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                nextWaiterObjId = waiterObj.getNextWaitingResourceObjId();
                entityInfo = waiterObj.getEntityInfoSlot();
                if (IS_DEBUG_MODE) {
                    if (jobId.getId() != entityInfoManager.getJobId(entityInfo)) {
                        throw new IllegalStateException("JobInfo(" + jobId + ") has diffrent Job(JID:"
                                + entityInfoManager.getJobId(entityInfo) + "'s lock request!!!");
                    }
                }

                //1. remove from waiter(or upgrader)'s list of dLockInfo or eLockInfo.
                did = entityInfoManager.getDatasetId(entityInfo);
                tempDatasetIdObj.setId(did);
                dLockInfo = datasetResourceHT.get(tempDatasetIdObj);

                if (waiterObj.isWaitingOnEntityLock()) {
                    entityHashValue = entityInfoManager.getPKHashVal(entityInfo);
                    eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);
                    if (waiterObj.isWaiter()) {
                        entityLockInfoManager.removeWaiter(eLockInfo, waiterObjId);
                    } else {
                        entityLockInfoManager.removeUpgrader(eLockInfo, waiterObjId);
                    }
                } else {
                    if (waiterObj.isWaiter()) {
                        dLockInfo.removeWaiter(waiterObjId);
                    } else {
                        dLockInfo.removeUpgrader(waiterObjId);
                    }
                }

                //2. wake-up waiters
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    if (IS_DEBUG_MODE) {
                        System.out.println("" + Thread.currentThread().getName() + "\twake-up(D): WID(" + waiterObjId
                                + "),EID(" + waiterObj.getEntityInfoSlot() + ")");
                    }
                    waiterObj.notifyAll();
                }

                //3. deallocate waiterObj
                lockWaiterManager.deallocate(waiterObjId);

                //4. deallocate entityInfo only if this waiter is not an upgrader
                if (entityInfoManager.getDatasetLockCount(entityInfo) == 0
                        && entityInfoManager.getEntityLockCount(entityInfo) == 0) {
                    entityInfoManager.deallocate(entityInfo);
                }
                waiterObjId = nextWaiterObjId;
            }

            //release holding resources
            entityInfo = jobInfo.getLastHoldingResource();
            while (entityInfo != -1) {
                prevEntityInfo = entityInfoManager.getPrevJobResource(entityInfo);

                //decrease lock count of datasetLock and entityLock
                did = entityInfoManager.getDatasetId(entityInfo);
                tempDatasetIdObj.setId(did);
                dLockInfo = datasetResourceHT.get(tempDatasetIdObj);
                entityHashValue = entityInfoManager.getPKHashVal(entityInfo);

                if (entityHashValue == -1) {
                    //decrease datasetLockCount
                    lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                    datasetLockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                    if (datasetLockCount != 0) {
                        dLockInfo.decreaseLockCount(lockMode, datasetLockCount);

                        //wakeup waiters of datasetLock and remove holder from datasetLockInfo
                        wakeUpDatasetLockWaiters(dLockInfo);

                        //remove the holder from datasetLockInfo only if the lock is dataset-granule lock.
                        //--> this also removes the holding resource from jobInfo               
                        //(Because the IX and IS lock's holders are handled implicitly, 
                        //those are not in the holder list of datasetLockInfo.)
                        dLockInfo.removeHolder(entityInfo, jobInfo);
                    }
                } else {
                    //decrease datasetLockCount
                    lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                    lockMode = lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                    datasetLockCount = entityInfoManager.getDatasetLockCount(entityInfo);

                    if (datasetLockCount != 0) {
                        dLockInfo.decreaseLockCount(lockMode, datasetLockCount);
                    }

                    //decrease entityLockCount
                    lockMode = entityInfoManager.getEntityLockMode(entityInfo);
                    entityLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                    eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);
                    if (IS_DEBUG_MODE) {
                        if (eLockInfo < 0) {
                            System.out.println("eLockInfo:" + eLockInfo);
                        }
                    }

                    if (entityLockCount != 0) {
                        entityLockInfoManager.decreaseLockCount(eLockInfo, lockMode, (short) entityLockCount);
                    }

                    if (datasetLockCount != 0) {
                        //wakeup waiters of datasetLock and don't remove holder from datasetLockInfo
                        wakeUpDatasetLockWaiters(dLockInfo);
                    }

                    if (entityLockCount != 0) {
                        //wakeup waiters of entityLock
                        wakeUpEntityLockWaiters(eLockInfo);

                        //remove the holder from entityLockInfo 
                        //--> this also removes the holding resource from jobInfo
                        entityLockInfoManager.removeHolder(eLockInfo, entityInfo, jobInfo);
                    }

                    //deallocate entityLockInfo if there is no holder and waiter.
                    if (entityLockInfoManager.getLastHolder(eLockInfo) == -1
                            && entityLockInfoManager.getFirstWaiter(eLockInfo) == -1
                            && entityLockInfoManager.getUpgrader(eLockInfo) == -1) {
                        dLockInfo.getEntityResourceHT().remove(entityHashValue);
                        entityLockInfoManager.deallocate(eLockInfo);
                    }
                }

                //deallocate entityInfo
                entityInfoManager.deallocate(entityInfo);

                entityInfo = prevEntityInfo;
            }

            //remove JobInfo
            jobHT.remove(jobId);

            if (existWaiter) {
                txnContext.setTimeout(true);
                txnContext.setTxnState(ITransactionManager.ABORTED);
            }

            if (IS_DEBUG_MODE) {
                trackLockRequest("Granted", RequestType.RELEASE_LOCKS, new DatasetId(0), 0, (byte) 0, txnContext,
                        dLockInfo, eLockInfo);
            }
        } finally {
            unlatchLockTable();
        }
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
        internalLock(datasetId, entityHashValue, lockMode, txnContext, true);
        instantUnlock(datasetId, entityHashValue, txnContext);
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, ITransactionContext txnContext)
            throws ACIDException {
        return internalTryLock(datasetId, entityHashValue, lockMode, txnContext, false);
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        return internalInstantTryLock(datasetId, entityHashValue, lockMode, txnContext);
    }

    private boolean internalInstantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        DatasetLockInfo dLockInfo = null;
        boolean isSuccess = true;

        latchLockTable();
        try {
            validateJob(txnContext);

            if (IS_DEBUG_MODE) {
                trackLockRequest("Requested", RequestType.INSTANT_TRY_LOCK, datasetId, entityHashValue, lockMode,
                        txnContext, dLockInfo, -1);
            }

            dLockInfo = datasetResourceHT.get(datasetId);

            //#. if the datasetLockInfo doesn't exist in datasetResourceHT 
            if (dLockInfo == null || dLockInfo.isNoHolder()) {
                if (IS_DEBUG_MODE) {
                    trackLockRequest("Granted", RequestType.INSTANT_TRY_LOCK, datasetId, entityHashValue, lockMode,
                            txnContext, dLockInfo, -1);
                }
                return true;
            }

            //#. the datasetLockInfo exists in datasetResourceHT.
            //1. handle dataset-granule lock
            byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS
                    : LockMode.IX;
            if (datasetLockMode == LockMode.IS) {
                //[Notice]
                //Skip checking the dataset level lock compatibility if the requested LockMode is IS lock.
                //We know that this internalInstantTryLock() call with IS lock mode will be always granted 
                //because we don't allow X lock on dataset-level except DDL operation. 
                //During DDL operation, all other operations will be pending, so there is no conflict. 
                isSuccess = true;
            } else {
                isSuccess = instantTryLockDatasetGranule(datasetId, entityHashValue, lockMode, txnContext, dLockInfo,
                        datasetLockMode);
            }

            if (isSuccess && entityHashValue != -1) {
                //2. handle entity-granule lock
                isSuccess = instantTryLockEntityGranule(datasetId, entityHashValue, lockMode, txnContext, dLockInfo);
            }

            if (IS_DEBUG_MODE) {
                if (isSuccess) {
                    trackLockRequest("Granted", RequestType.INSTANT_TRY_LOCK, datasetId, entityHashValue, lockMode,
                            txnContext, dLockInfo, -1);
                } else {
                    trackLockRequest("Failed", RequestType.INSTANT_TRY_LOCK, datasetId, entityHashValue, lockMode,
                            txnContext, dLockInfo, -1);
                }
            }

        } finally {
            unlatchLockTable();
        }

        return isSuccess;
    }

    private boolean instantTryLockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext, DatasetLockInfo dLockInfo, byte datasetLockMode) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int waiterObjId;
        int entityInfo = -1;
        JobInfo jobInfo;
        boolean isUpgrade = false;

        jobInfo = jobHT.get(jobId);

        //check duplicated call

        //1. lock request causing duplicated upgrading requests from different threads in a same job
        waiterObjId = dLockInfo.findUpgraderFromUpgraderList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return false;
        }

        //2. lock request causing duplicated waiting requests from different threads in a same job
        waiterObjId = dLockInfo.findWaiterFromWaiterList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return false;
        }

        //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
        entityInfo = dLockInfo.findEntityInfoFromHolderList(jId, entityHashValue);
        if (entityInfo == -1) { //new call from this job -> doesn't mean that eLockInfo doesn't exist since another thread might have create the eLockInfo already.

            //return fail if any upgrader exists or upgrading lock mode is not compatible
            if (dLockInfo.getFirstUpgrader() != -1 || dLockInfo.getFirstWaiter() != -1
                    || !dLockInfo.isCompatible(datasetLockMode)) {

                if (ALLOW_DATASET_GRANULE_X_LOCK_WITH_OTHER_CONCURRENT_LOCK_REQUESTS) {
                    //The following case only may occur when the dataset level X lock is requested 
                    //with the other lock

                    //[Notice]
                    //There has been no same caller as (jId, dId, entityHashValue) triplet.
                    //But there could be the same caller in terms of (jId, dId) pair.
                    //For example, 
                    //1) (J1, D1, E1) acquires IS in Dataset D1
                    //2) (J2, D1, -1) requests X  in Dataset D1, but waits
                    //3) (J1, D1, E2) requests IS in Dataset D1, but should wait 
                    //The 3) may cause deadlock if 1) and 3) are under the same thread.
                    //Even if (J1, D1, E1) and (J1, D1, E2) are two different thread, instead of
                    //aborting (J1, D1, E1) triggered by the deadlock, we give higher priority to 3) than 2)
                    //as long as the dataset level lock D1 is being held by the same jobId. 
                    //The above consideration is covered in the following code.
                    //find the same dataset-granule lock request, that is, (J1, D1) pair in the above example.
                    if (jobInfo != null && jobInfo.isDatasetLockGranted(dId, LockMode.IS)) {
                        if (dLockInfo.isCompatible(datasetLockMode)) {
                            //this is duplicated call
                            return true;
                        }
                    }
                }

                return false;
            }
        } else {
            isUpgrade = isLockUpgrade(entityInfoManager.getDatasetLockMode(entityInfo), lockMode);
            if (isUpgrade) { //upgrade call 
                //return fail if any upgrader exists or upgrading lock mode is not compatible
                if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                    return false;
                }
            }
            /************************************
             * else { //duplicated call
             * //do nothing
             * }
             *************************************/
        }

        return true;
    }

    private boolean instantTryLockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext, DatasetLockInfo dLockInfo) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int waiterObjId;
        int eLockInfo = -1;
        int entityInfo;
        boolean isUpgrade = false;

        dLockInfo = datasetResourceHT.get(datasetId);
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

        if (eLockInfo != -1) {
            //check duplicated call

            //1. lock request causing duplicated upgrading requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findUpgraderFromUpgraderList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //2. lock request causing duplicated waiting requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jId, entityHashValue);
            if (entityInfo != -1) {//duplicated call or upgrader

                isUpgrade = isLockUpgrade(entityInfoManager.getEntityLockMode(entityInfo), lockMode);
                if (isUpgrade) {//upgrade call
                    //wait if any upgrader exists or upgrading lock mode is not compatible
                    if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                            || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                        return false;
                    }
                }
                /***************************
                 * else {//duplicated call
                 * //do nothing
                 * }
                 ****************************/
            } else {//new call from this job, but still eLockInfo exists since other threads hold it or wait on it
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                        || entityLockInfoManager.getFirstWaiter(eLockInfo) != -1
                        || !entityLockInfoManager.isCompatible(eLockInfo, lockMode)) {
                    return false;
                }
            }
        }
        /*******************************
         * else {//eLockInfo doesn't exist, so this lock request is the first request and can be granted without waiting.
         * //do nothing
         * }
         *********************************/

        return true;
    }

    private boolean internalTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext, boolean isInstant) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int entityInfo;
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
        boolean isSuccess = true;
        boolean doEscalate = false;

        latchLockTable();
        try {
            validateJob(txnContext);

            if (IS_DEBUG_MODE) {
                trackLockRequest("Requested", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext,
                        dLockInfo, eLockInfo);
            }

            dLockInfo = datasetResourceHT.get(datasetId);
            jobInfo = jobHT.get(jobId);

            if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                if (!isInstant && datasetLockMode == LockMode.IS && jobInfo != null && dLockInfo != null) {
                    int upgradeStatus = needEscalateFromEntityToDataset(jobInfo, dId, lockMode);
                    switch (upgradeStatus) {
                        case DO_ESCALATE:
                            entityHashValue = -1;
                            doEscalate = true;
                            break;

                        case ESCALATED:
                            return true;

                        default:
                            break;
                    }
                }
            }

            //#. if the datasetLockInfo doesn't exist in datasetResourceHT 
            if (dLockInfo == null || dLockInfo.isNoHolder()) {
                if (dLockInfo == null) {
                    dLockInfo = new DatasetLockInfo(entityLockInfoManager, entityInfoManager, lockWaiterManager);
                    datasetResourceHT.put(new DatasetId(dId), dLockInfo); //datsetId obj should be created
                }
                entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);

                //if dataset-granule lock
                if (entityHashValue == -1) { //-1 stands for dataset-granule
                    entityInfoManager.increaseDatasetLockCount(entityInfo);
                    dLockInfo.increaseLockCount(datasetLockMode);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    entityInfoManager.increaseDatasetLockCount(entityInfo);
                    dLockInfo.increaseLockCount(datasetLockMode);
                    //add entityLockInfo
                    eLockInfo = entityLockInfoManager.allocate();
                    dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }

                if (jobInfo == null) {
                    jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                    jobHT.put(jobId, jobInfo); //jobId obj doesn't have to be created
                }
                jobInfo.addHoldingResource(entityInfo);

                if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                    if (!isInstant && datasetLockMode == LockMode.IS) {
                        jobInfo.increaseDatasetISLockCount(dId);
                        if (doEscalate) {
                            //This exception is thrown when the threshold value is set to 1.
                            //We don't want to allow the lock escalation when there is a first lock request on a dataset. 
                            throw new IllegalStateException(
                                    "ESCALATE_TRHESHOLD_ENTITY_TO_DATASET should not be set to "
                                            + txnSubsystem.getTransactionProperties()
                                                    .getEntityToDatasetLockEscalationThreshold());
                        }
                    }
                }

                if (IS_DEBUG_MODE) {
                    trackLockRequest("Granted", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext,
                            dLockInfo, eLockInfo);
                }

                return true;
            }

            //#. the datasetLockInfo exists in datasetResourceHT.
            //1. handle dataset-granule lock
            tryLockDatasetGranuleRevertOperation = 0;
            entityInfo = tryLockDatasetGranule(datasetId, entityHashValue, lockMode, txnContext);
            if (entityInfo == -2) {//-2 represents fail
                isSuccess = false;
            } else {
                //2. handle entity-granule lock
                if (entityHashValue != -1) {
                    isSuccess = tryLockEntityGranule(datasetId, entityHashValue, lockMode, entityInfo, txnContext);
                    if (!isSuccess) {
                        revertTryLockDatasetGranuleOperation(datasetId, entityHashValue, lockMode, entityInfo,
                                txnContext);
                    }
                }
            }

            if (ALLOW_ESCALATE_FROM_ENTITY_TO_DATASET) {
                if (!isInstant) {
                    if (doEscalate) {
                        //jobInfo must not be null.
                        assert jobInfo != null;
                        jobInfo.increaseDatasetISLockCount(dId);
                        //release pre-acquired locks
                        releaseDatasetISLocks(jobInfo, jobId, datasetId, txnContext);
                    } else if (datasetLockMode == LockMode.IS) {
                        if (jobInfo == null) {
                            jobInfo = jobHT.get(jobId);
                            //jobInfo must not be null;
                            assert jobInfo != null;
                        }
                        jobInfo.increaseDatasetISLockCount(dId);
                    }
                }
            }

            if (IS_DEBUG_MODE) {
                if (isSuccess) {
                    trackLockRequest("Granted", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext,
                            dLockInfo, eLockInfo);
                } else {
                    trackLockRequest("Failed", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext,
                            dLockInfo, eLockInfo);
                }
            }

        } finally {
            unlatchLockTable();
        }

        return isSuccess;
    }

    private void trackLockRequest(String msg, int requestType, DatasetId datasetIdObj, int entityHashValue,
            byte lockMode, ITransactionContext txnContext, DatasetLockInfo dLockInfo, int eLockInfo) {
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
    }

    public String getHistoryForAllJobs() {
        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getHistoryForAllJobs();
        }
        return null;
    }

    public String getHistoryPerJob() {
        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getHistoryPerJob();
        }
        return null;
    }

    public String getRequestHistoryForAllJobs() {
        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getRequestHistoryForAllJobs();
        }
        return null;
    }

    private void revertTryLockDatasetGranuleOperation(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfo, ITransactionContext txnContext) {
        JobId jobId = txnContext.getJobId();
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        int lockCount;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //see tryLockDatasetGranule() function to know the revert operation
        switch (tryLockDatasetGranuleRevertOperation) {

            case 1://[revertOperation1]: reverting 'adding a holder'

                if (entityHashValue == -1) {
                    dLockInfo.decreaseLockCount(datasetLockMode);
                    dLockInfo.removeHolder(entityInfo, jobInfo); //--> this call removes entityInfo from JobInfo's holding-resource-list as well.
                } else {
                    dLockInfo.decreaseLockCount(datasetLockMode);
                    jobInfo.removeHoldingResource(entityInfo);
                }
                entityInfoManager.decreaseDatasetLockCount(entityInfo);
                if (jobInfo.getLastHoldingResource() == -1 && jobInfo.getFirstWaitingResource() == -1) {
                    jobHT.remove(jobId);
                }
                entityInfoManager.deallocate(entityInfo);
                break;

            case 2://[revertOperation2]: reverting 'adding an upgrader'
                lockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.decreaseLockCount(LockMode.X, lockCount);
                    dLockInfo.increaseLockCount(LockMode.S, lockCount);
                } else {
                    dLockInfo.decreaseLockCount(LockMode.IX, lockCount);
                    dLockInfo.increaseLockCount(LockMode.IS, lockCount);
                }
                entityInfoManager.setDatasetLockMode(entityInfo, LockMode.S);
                break;

            case 3://[revertOperation3]: reverting 'adding a duplicated call'
                entityInfoManager.decreaseDatasetLockCount(entityInfo);
                datasetLockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.decreaseLockCount(datasetLockMode);
                } else { //entity-granule
                    datasetLockMode = datasetLockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                    dLockInfo.decreaseLockCount(datasetLockMode);
                }

                break;
            default:
                //do nothing;
        }
    }

    private int tryLockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            ITransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int waiterObjId;
        int entityInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int weakerModeLockCount;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //check duplicated call

        //1. lock request causing duplicated upgrading requests from different threads in a same job
        waiterObjId = dLockInfo.findUpgraderFromUpgraderList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return -2;
        }

        //2. lock request causing duplicated waiting requests from different threads in a same job
        waiterObjId = dLockInfo.findWaiterFromWaiterList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return -2;
        }

        //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
        entityInfo = dLockInfo.findEntityInfoFromHolderList(jId, entityHashValue);
        if (entityInfo == -1) { //new call from this job -> doesn't mean that eLockInfo doesn't exist since another thread might have create the eLockInfo already.

            //////////////////////////////////////////////////////////////////////////////////////
            //[part of revertOperation1]
            entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);
            if (jobInfo == null) {
                jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                jobHT.put(jobId, jobInfo);
            }
            //////////////////////////////////////////////////////////////////////////////////////

            //return fail if any upgrader exists or upgrading lock mode is not compatible
            if (dLockInfo.getFirstUpgrader() != -1 || dLockInfo.getFirstWaiter() != -1
                    || !dLockInfo.isCompatible(datasetLockMode)) {

                if (ALLOW_DATASET_GRANULE_X_LOCK_WITH_OTHER_CONCURRENT_LOCK_REQUESTS) {
                    //The following case only may occur when the dataset level X lock is requested 
                    //with the other lock

                    //[Notice]
                    //There has been no same caller as (jId, dId, entityHashValue) triplet.
                    //But there could be the same caller in terms of (jId, dId) pair.
                    //For example, 
                    //1) (J1, D1, E1) acquires IS in Dataset D1
                    //2) (J2, D1, -1) requests X  in Dataset D1, but waits
                    //3) (J1, D1, E2) requests IS in Dataset D1, but should wait 
                    //The 3) may cause deadlock if 1) and 3) are under the same thread.
                    //Even if (J1, D1, E1) and (J1, D1, E2) are two different thread, instead of
                    //aborting (J1, D1, E1) triggered by the deadlock, we give higher priority to 3) than 2)
                    //as long as the dataset level lock D1 is being held by the same jobId. 
                    //The above consideration is covered in the following code.
                    //find the same dataset-granule lock request, that is, (J1, D1) pair in the above example.
                    if (jobInfo.isDatasetLockGranted(dId, LockMode.IS)) {
                        if (dLockInfo.isCompatible(datasetLockMode)) {
                            //this is duplicated call
                            entityInfoManager.increaseDatasetLockCount(entityInfo);
                            if (entityHashValue == -1) {
                                dLockInfo.increaseLockCount(datasetLockMode);
                                dLockInfo.addHolder(entityInfo);
                            } else {
                                dLockInfo.increaseLockCount(datasetLockMode);
                                //IS and IX holders are implicitly handled.
                            }
                            //add entityInfo to JobInfo's holding-resource list
                            jobInfo.addHoldingResource(entityInfo);

                            tryLockDatasetGranuleRevertOperation = 1;

                            return entityInfo;
                        }
                    }
                }

                //revert [part of revertOperation1] before return
                if (jobInfo.getLastHoldingResource() == -1 && jobInfo.getFirstWaitingResource() == -1) {
                    jobHT.remove(jobId);
                }
                entityInfoManager.deallocate(entityInfo);

                return -2;
            }

            //////////////////////////////////////////////////////////////////////////////////////
            //revert the following operations if the caller thread has to wait during this call.
            //[revertOperation1]
            entityInfoManager.increaseDatasetLockCount(entityInfo);
            if (entityHashValue == -1) {
                dLockInfo.increaseLockCount(datasetLockMode);
                dLockInfo.addHolder(entityInfo);
            } else {
                dLockInfo.increaseLockCount(datasetLockMode);
                //IS and IX holders are implicitly handled.
            }
            //add entityInfo to JobInfo's holding-resource list
            jobInfo.addHoldingResource(entityInfo);

            //set revert operation to be reverted when tryLock() fails
            tryLockDatasetGranuleRevertOperation = 1;
            //////////////////////////////////////////////////////////////////////////////////////

        } else {
            isUpgrade = isLockUpgrade(entityInfoManager.getDatasetLockMode(entityInfo), lockMode);
            if (isUpgrade) { //upgrade call 
                //return fail if any upgrader exists or upgrading lock mode is not compatible
                if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                    return -2;
                }

                //update entityInfo's dataset lock count and datasetLockInfo's lock count
                weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);

                //////////////////////////////////////////////////////////////////////////////////////
                //revert the following operations if the caller thread has to wait during this call.
                //[revertOperation2]
                entityInfoManager.setDatasetLockMode(entityInfo, lockMode);

                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount);//new lock mode
                    dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                } else {
                    dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount);
                    dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                }
                tryLockDatasetGranuleRevertOperation = 2;
                //////////////////////////////////////////////////////////////////////////////////////

            } else { //duplicated call

                //////////////////////////////////////////////////////////////////////////////////////
                //revert the following operations if the caller thread has to wait during this call.
                //[revertOperation3]
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                datasetLockMode = entityInfoManager.getDatasetLockMode(entityInfo);

                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                } else { //entity-granule
                    datasetLockMode = datasetLockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                    dLockInfo.increaseLockCount(datasetLockMode);
                }

                tryLockDatasetGranuleRevertOperation = 3;
                //////////////////////////////////////////////////////////////////////////////////////

            }
        }

        return entityInfo;
    }

    private boolean tryLockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfoFromDLockInfo, ITransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int waiterObjId;
        int eLockInfo = -1;
        int entityInfo;
        DatasetLockInfo dLockInfo;
        boolean isUpgrade = false;
        int weakerModeLockCount;

        dLockInfo = datasetResourceHT.get(datasetId);
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

        if (eLockInfo != -1) {
            //check duplicated call

            //1. lock request causing duplicated upgrading requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findUpgraderFromUpgraderList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //2. lock request causing duplicated waiting requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jId, entityHashValue);
            if (entityInfo != -1) {//duplicated call or upgrader

                isUpgrade = isLockUpgrade(entityInfoManager.getEntityLockMode(entityInfo), lockMode);
                if (isUpgrade) {//upgrade call
                    //wait if any upgrader exists or upgrading lock mode is not compatible
                    if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                            || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                        return false;
                    }

                    weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                    entityInfoManager.setEntityLockMode(entityInfo, lockMode);

                    entityLockInfoManager.increaseLockCount(eLockInfo, LockMode.X, (short) weakerModeLockCount);//new lock mode
                    entityLockInfoManager.decreaseLockCount(eLockInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode

                } else {//duplicated call
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, entityInfoManager.getEntityLockMode(entityInfo));
                }
            } else {//new call from this job, but still eLockInfo exists since other threads hold it or wait on it
                entityInfo = entityInfoFromDLockInfo;
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                        || entityLockInfoManager.getFirstWaiter(eLockInfo) != -1
                        || !entityLockInfoManager.isCompatible(eLockInfo, lockMode)) {
                    return false;
                }

                entityInfoManager.increaseEntityLockCount(entityInfo);
                entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                entityLockInfoManager.addHolder(eLockInfo, entityInfo);
            }
        } else {//eLockInfo doesn't exist, so this lock request is the first request and can be granted without waiting.
            eLockInfo = entityLockInfoManager.allocate();
            dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
            entityInfoManager.increaseEntityLockCount(entityInfoFromDLockInfo);
            entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
            entityLockInfoManager.addHolder(eLockInfo, entityInfoFromDLockInfo);
        }

        return true;
    }

    private void latchLockTable() {
        lockTableLatch.writeLock().lock();
    }

    private void unlatchLockTable() {
        lockTableLatch.writeLock().unlock();
    }

    private void latchWaitNotify() {
        waiterLatch.writeLock().lock();
    }

    private void unlatchWaitNotify() {
        waiterLatch.writeLock().unlock();
    }

    private int handleLockWaiter(DatasetLockInfo dLockInfo, int eLockInfo, int entityInfo, boolean isUpgrade,
            boolean isDatasetLockInfo, ITransactionContext txnContext, JobInfo jobInfo, int duplicatedWaiterObjId)
            throws ACIDException {
        int waiterId = -1;
        LockWaiter waiter;
        int waiterCount = 0;
        boolean isInterruptedExceptionOccurred = false;

        if (duplicatedWaiterObjId != -1
                || isDeadlockFree(dLockInfo, eLockInfo, entityInfo, isDatasetLockInfo, isUpgrade)) {//deadlock free -> wait
            if (duplicatedWaiterObjId == -1) {
                waiterId = lockWaiterManager.allocate(); //initial value of waiterObj: wait = true, victim = false
                waiter = lockWaiterManager.getLockWaiter(waiterId);
                waiter.setEntityInfoSlot(entityInfo);
                jobInfo.addWaitingResource(waiterId);
                waiter.setBeginWaitTime(System.currentTimeMillis());
            } else {
                waiterId = duplicatedWaiterObjId;
                waiter = lockWaiterManager.getLockWaiter(waiterId);
            }

            if (duplicatedWaiterObjId == -1) {
                //add actor properly
                if (isDatasetLockInfo) {
                    waiter.setWaitingOnEntityLock(false);
                    if (isUpgrade) {
                        dLockInfo.addUpgrader(waiterId);
                        waiter.setWaiter(false);
                    } else {
                        dLockInfo.addWaiter(waiterId);
                        waiter.setWaiter(true);
                    }
                } else {
                    waiter.setWaitingOnEntityLock(true);
                    if (isUpgrade) {
                        waiter.setWaiter(false);
                        entityLockInfoManager.addUpgrader(eLockInfo, waiterId);
                    } else {
                        waiter.setWaiter(true);
                        entityLockInfoManager.addWaiter(eLockInfo, waiterId);
                    }
                }
            }
            waiter.increaseWaiterCount();
            waiter.setFirstGetUp(true);

            latchWaitNotify();
            unlatchLockTable();
            try {
                synchronized (waiter) {
                    unlatchWaitNotify();
                    while (waiter.needWait()) {
                        try {
                            if (IS_DEBUG_MODE) {
                                System.out.println("" + Thread.currentThread().getName() + "\twaits("
                                        + waiter.getWaiterCount() + "): WID(" + waiterId + "),EID("
                                        + waiter.getEntityInfoSlot() + ")");
                            }
                            waiter.wait();
                        } catch (InterruptedException e) {
                            //TODO figure-out what is the appropriate way to handle this exception
                            e.printStackTrace();
                            isInterruptedExceptionOccurred = true;
                            waiter.setWait(false);
                        }
                    }
                }

                if (isInterruptedExceptionOccurred) {
                    throw new ACIDException("InterruptedException is caught");
                }
            } catch (Exception e) {
                throw new LockMgrLatchHandlerException(e);
            }

            //waiter woke up -> remove/deallocate waiter object and abort if timeout
            latchLockTable();

            if (txnContext.isTimeout() || waiter.isVictim()) {
                requestAbort(txnContext);
            }

            if (waiter.isFirstGetUp()) {
                waiter.setFirstGetUp(false);
                waiterCount = waiter.getWaiterCount();
            } else {
                waiterCount = 0;
            }

            waiter.decreaseWaiterCount();
            if (IS_DEBUG_MODE) {
                System.out.println("" + Thread.currentThread().getName() + "\tgot-up!(" + waiter.getWaiterCount()
                        + "): WID(" + waiterId + "),EID(" + waiter.getEntityInfoSlot() + ")");
            }
            if (waiter.getWaiterCount() == 0) {
                //remove actor properly
                if (isDatasetLockInfo) {
                    if (isUpgrade) {
                        dLockInfo.removeUpgrader(waiterId);
                    } else {
                        dLockInfo.removeWaiter(waiterId);
                    }
                } else {
                    if (isUpgrade) {
                        entityLockInfoManager.removeUpgrader(eLockInfo, waiterId);
                    } else {
                        entityLockInfoManager.removeWaiter(eLockInfo, waiterId);
                    }
                }

                //if (!isUpgrade && isDatasetLockInfo) {
                jobInfo.removeWaitingResource(waiterId);
                //}
                lockWaiterManager.deallocate(waiterId);
            }

        } else { //deadlock -> abort
            //[Notice]
            //Before requesting abort, the entityInfo for waiting datasetLock request is deallocated.
            if (!isUpgrade && isDatasetLockInfo) {
                //deallocate the entityInfo
                entityInfoManager.deallocate(entityInfo);
            }
            requestAbort(txnContext);
        }

        return waiterCount;
    }

    private boolean isDeadlockFree(DatasetLockInfo dLockInfo, int eLockInfo, int entityInfo, boolean isDatasetLockInfo,
            boolean isUpgrade) {
        return deadlockDetector.isSafeToAdd(dLockInfo, eLockInfo, entityInfo, isDatasetLockInfo, isUpgrade);
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

    /**
     * wake up upgraders first, then waiters.
     * Criteria to wake up upgraders: if the upgrading lock mode is compatible, then wake up the upgrader.
     */
    private void wakeUpDatasetLockWaiters(DatasetLockInfo dLockInfo) {
        int waiterObjId = dLockInfo.getFirstUpgrader();
        int entityInfo;
        LockWaiter waiterObj;
        byte datasetLockMode;
        byte lockMode;
        boolean areAllUpgradersAwaken = true;

        consecutiveWakeupContext.reset();
        while (waiterObjId != -1) {
            //wake up upgraders
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? LockMode.X : LockMode.IX;
            if (dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)
                    && consecutiveWakeupContext.isCompatible(datasetLockMode)) {
                consecutiveWakeupContext.setLockMode(datasetLockMode);
                //compatible upgrader is waken up
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    if (IS_DEBUG_MODE) {
                        System.out.println("" + Thread.currentThread().getName() + "\twake-up(D): WID(" + waiterObjId
                                + "),EID(" + waiterObj.getEntityInfoSlot() + ")");
                    }
                    waiterObj.notifyAll();
                }
                waiterObjId = waiterObj.getNextWaiterObjId();
            } else {
                areAllUpgradersAwaken = false;
                break;
            }
        }

        if (areAllUpgradersAwaken) {
            //wake up waiters
            waiterObjId = dLockInfo.getFirstWaiter();
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? lockMode
                        : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                if (dLockInfo.isCompatible(datasetLockMode) && consecutiveWakeupContext.isCompatible(datasetLockMode)) {
                    consecutiveWakeupContext.setLockMode(datasetLockMode);
                    //compatible waiter is waken up
                    latchWaitNotify();
                    synchronized (waiterObj) {
                        unlatchWaitNotify();
                        waiterObj.setWait(false);
                        if (IS_DEBUG_MODE) {
                            System.out.println("" + Thread.currentThread().getName() + "\twake-up(D): WID("
                                    + waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot() + ")");
                        }
                        waiterObj.notifyAll();
                    }
                    waiterObjId = waiterObj.getNextWaiterObjId();
                } else {
                    break;
                }
            }
        }
    }

    private void wakeUpEntityLockWaiters(int eLockInfo) {
        boolean areAllUpgradersAwaken = true;
        int waiterObjId = entityLockInfoManager.getUpgrader(eLockInfo);
        int entityInfo;
        LockWaiter waiterObj;
        byte entityLockMode;

        consecutiveWakeupContext.reset();
        while (waiterObjId != -1) {
            //wake up upgraders
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            if (entityLockInfoManager.isUpgradeCompatible(eLockInfo, LockMode.X, entityInfo)
                    && consecutiveWakeupContext.isCompatible(LockMode.X)) {
                consecutiveWakeupContext.setLockMode(LockMode.X);
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    if (IS_DEBUG_MODE) {
                        System.out.println("" + Thread.currentThread().getName() + "\twake-up(E): WID(" + waiterObjId
                                + "),EID(" + waiterObj.getEntityInfoSlot() + ")");
                    }
                    waiterObj.notifyAll();
                }
                waiterObjId = waiterObj.getNextWaiterObjId();
            } else {
                areAllUpgradersAwaken = false;
                break;
            }
        }

        if (areAllUpgradersAwaken) {
            //wake up waiters
            waiterObjId = entityLockInfoManager.getFirstWaiter(eLockInfo);
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                entityLockMode = entityInfoManager.getEntityLockMode(entityInfo);
                if (entityLockInfoManager.isCompatible(eLockInfo, entityLockMode)
                        && consecutiveWakeupContext.isCompatible(entityLockMode)) {
                    consecutiveWakeupContext.setLockMode(entityLockMode);
                    //compatible waiter is waken up
                    latchWaitNotify();
                    synchronized (waiterObj) {
                        unlatchWaitNotify();
                        waiterObj.setWait(false);
                        if (IS_DEBUG_MODE) {
                            System.out.println("" + Thread.currentThread().getName() + "\twake-up(E): WID("
                                    + waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot() + ")");
                        }
                        waiterObj.notifyAll();
                    }
                } else {
                    break;
                }
                waiterObjId = waiterObj.getNextWaiterObjId();
            }
        }
    }

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return s + "\n";
    }

    public void sweepForTimeout() throws ACIDException {
        JobInfo jobInfo;
        int waiterObjId;
        LockWaiter waiterObj;

        latchLockTable();
        try {

            Iterator<Entry<JobId, JobInfo>> iter = jobHT.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<JobId, JobInfo> pair = (Map.Entry<JobId, JobInfo>) iter.next();
                jobInfo = pair.getValue();
                waiterObjId = jobInfo.getFirstWaitingResource();
                while (waiterObjId != -1) {
                    waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                    toutDetector.checkAndSetVictim(waiterObj);
                    waiterObjId = waiterObj.getNextWaiterObjId();
                }
            }
        } finally {
            unlatchLockTable();
        }
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
            sb.append("\nSHRINK_TIMER_THRESHOLD (entityLockInfoManager): "
                    + entityLockInfoManager.getShrinkTimerThreshold());
            sb.append("\nSHRINK_TIMER_THRESHOLD (entityInfoManager): " + entityInfoManager.getShrinkTimerThreshold());
            sb.append("\nSHRINK_TIMER_THRESHOLD (lockWaiterManager): " + lockWaiterManager.getShrinkTimerThreshold());
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
    }

    private void dumpDatasetLockInfo(OutputStream os) {
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
    }

    private void dumpEntityLockInfo(OutputStream os) {
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
    }

    private void dumpEntityInfo(OutputStream os) {
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
    }

    private void dumpLockWaiterInfo(OutputStream os) {
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
    }

    public void batchUnlock(LogPage logPage, LogPageReader logPageReader) throws ACIDException {
        latchLockTable();
        try {
            ITransactionContext txnCtx = null;
            LogRecord logRecord = logPageReader.next();
            while (logRecord != null) {
                if (logRecord.getLogType() == LogType.ENTITY_COMMIT) {
                    tempDatasetIdObj.setId(logRecord.getDatasetId());
                    tempJobIdObj.setId(logRecord.getJobId());
                    txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(tempJobIdObj, false);
                    unlock(tempDatasetIdObj, logRecord.getPKHashValue(), txnCtx);
                    txnCtx.notifyOptracker(false);
                } else if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT) {
                    tempJobIdObj.setId(logRecord.getJobId());
                    txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(tempJobIdObj, false);
                    txnCtx.notifyOptracker(true);
                    ((LogPage) logPage).notifyJobTerminator();
                }
                logRecord = logPageReader.next();
            }
        } finally {
            unlatchLockTable();
        }
    }
}

class ConsecutiveWakeupContext {
    private boolean IS;
    private boolean IX;
    private boolean S;
    private boolean X;

    public void reset() {
        IS = false;
        IX = false;
        S = false;
        X = false;
    }

    public boolean isCompatible(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                return !S && !X;

            case LockMode.IS:
                return !X;

            case LockMode.X:
                return !IS && !IX && !S && !X;

            case LockMode.S:
                return !IX && !X;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public void setLockMode(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                IX = true;
                return;

            case LockMode.IS:
                IS = true;
                return;

            case LockMode.X:
                X = true;
                return;

            case LockMode.S:
                S = true;
                return;

            default:
                throw new IllegalStateException("Invalid lock mode");
        }

    }

}
