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
package org.apache.asterix.app.active;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IRetryPolicy;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.StatsRequestMessage;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.metadata.api.IActiveEntityController;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;

public abstract class ActiveEntityEventsListener implements IActiveEntityController {

    private static final Logger LOGGER = Logger.getLogger(ActiveEntityEventsListener.class.getName());
    private static final ActiveEvent STATE_CHANGED = new ActiveEvent(null, Kind.STATE_CHANGED, null, null);
    private static final EnumSet<ActivityState> TRANSITION_STATES = EnumSet.of(ActivityState.RESUMING,
            ActivityState.STARTING, ActivityState.STOPPING, ActivityState.RECOVERING);
    // finals
    protected final IClusterStateManager clusterStateManager;
    protected final ActiveNotificationHandler handler;
    protected final List<IActiveEntityEventSubscriber> subscribers = new ArrayList<>();
    protected final IStatementExecutor statementExecutor;
    protected final ICcApplicationContext appCtx;
    protected final MetadataProvider metadataProvider;
    protected final IHyracksClientConnection hcc;
    protected final EntityId entityId;
    private final List<Dataset> datasets;
    protected final ActiveEvent statsUpdatedEvent;
    protected final String runtimeName;
    protected final IRetryPolicyFactory retryPolicyFactory;
    // mutables
    protected volatile ActivityState state;
    private AlgebricksAbsolutePartitionConstraint locations;
    protected ActivityState prevState;
    protected JobId jobId;
    protected long statsTimestamp;
    protected String stats;
    protected boolean isFetchingStats;
    protected int numRegistered;
    protected volatile Future<Void> recoveryTask;
    protected volatile boolean cancelRecovery;
    protected volatile boolean suspended = false;
    // failures
    protected Exception jobFailure;
    protected Exception resumeFailure;
    protected Exception startFailure;
    protected Exception stopFailure;
    protected Exception recoverFailure;

    public ActiveEntityEventsListener(IStatementExecutor statementExecutor, ICcApplicationContext appCtx,
            IHyracksClientConnection hcc, EntityId entityId, List<Dataset> datasets,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName, IRetryPolicyFactory retryPolicyFactory)
            throws HyracksDataException {
        this.statementExecutor = statementExecutor;
        this.appCtx = appCtx;
        this.clusterStateManager = appCtx.getClusterStateManager();
        this.metadataProvider = new MetadataProvider(appCtx, null);
        metadataProvider.setConfig(new HashMap<>());
        this.hcc = hcc;
        this.entityId = entityId;
        this.datasets = datasets;
        this.retryPolicyFactory = retryPolicyFactory;
        this.state = ActivityState.STOPPED;
        this.statsTimestamp = -1;
        this.isFetchingStats = false;
        this.statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId, null);
        this.stats = "{\"Stats\":\"N/A\"}";
        this.runtimeName = runtimeName;
        this.locations = locations;
        this.numRegistered = 0;
        this.handler =
                (ActiveNotificationHandler) metadataProvider.getApplicationContext().getActiveNotificationHandler();
        handler.registerListener(this);
    }

    protected synchronized void setState(ActivityState newState) {
        LOGGER.log(Level.FINE, "State is being set to " + newState + " from " + state);
        this.prevState = state;
        this.state = newState;
        if (newState == ActivityState.SUSPENDED) {
            suspended = true;
        }
        notifySubscribers(STATE_CHANGED);
    }

    @Override
    public synchronized void notify(ActiveEvent event) {
        try {
            LOGGER.fine("EventListener is notified.");
            ActiveEvent.Kind eventKind = event.getEventKind();
            switch (eventKind) {
                case JOB_CREATED:
                    jobCreated(event);
                    break;
                case JOB_STARTED:
                    start(event);
                    break;
                case JOB_FINISHED:
                    finish(event);
                    break;
                case PARTITION_EVENT:
                    handle((ActivePartitionMessage) event.getEventObject());
                    break;
                default:
                    LOGGER.log(Level.FINE, "Unhandled feed event notification: " + event);
                    break;
            }
            notifySubscribers(event);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    protected void jobCreated(ActiveEvent event) {
        // Do nothing
    }

    protected synchronized void handle(ActivePartitionMessage message) {
        if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == locations.getLocations().length) {
                setState(ActivityState.RUNNING);
            }
        } else if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_DEREGISTERED) {
            numRegistered--;
        }
    }

    @SuppressWarnings("unchecked")
    protected void finish(ActiveEvent event) throws HyracksDataException {
        jobId = null;
        Pair<JobStatus, List<Exception>> status = (Pair<JobStatus, List<Exception>>) event.getEventObject();
        JobStatus jobStatus = status.getLeft();
        List<Exception> exceptions = status.getRight();
        if (jobStatus.equals(JobStatus.FAILURE)) {
            jobFailure = exceptions.isEmpty() ? new RuntimeDataException(ErrorCode.UNREPORTED_TASK_FAILURE_EXCEPTION)
                    : exceptions.get(0);
            setState(ActivityState.TEMPORARILY_FAILED);
            if (prevState != ActivityState.SUSPENDING && prevState != ActivityState.RECOVERING) {
                recover();
            }
        } else {
            setState(state == ActivityState.SUSPENDING ? ActivityState.SUSPENDED : ActivityState.STOPPED);
        }
    }

    protected void start(ActiveEvent event) {
        this.jobId = event.getJobId();
        numRegistered = 0;
    }

    @Override
    public synchronized void subscribe(IActiveEntityEventSubscriber subscriber) throws HyracksDataException {
        subscriber.subscribed(this);
        if (!subscriber.isDone()) {
            subscribers.add(subscriber);
        }
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public ActivityState getState() {
        return state;
    }

    @Override
    public synchronized boolean isEntityUsingDataset(IDataset dataset) {
        return isActive() && getDatasets().contains(dataset);
    }

    @Override
    public synchronized void remove(Dataset dataset) throws HyracksDataException {
        if (isActive()) {
            throw new RuntimeDataException(ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY, entityId, state);
        }
        getDatasets().remove(dataset);
    }

    @Override
    public synchronized void add(Dataset dataset) throws HyracksDataException {
        if (isActive()) {
            throw new RuntimeDataException(ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY, entityId, state);
        }
        getDatasets().add(dataset);
    }

    public JobId getJobId() {
        return jobId;
    }

    @Override
    public String getStats() {
        return stats;
    }

    @Override
    public long getStatsTimeStamp() {
        return statsTimestamp;
    }

    public String formatStats(List<String> responses) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("{\"Stats\": [").append(responses.get(0));
        for (int i = 1; i < responses.size(); i++) {
            strBuilder.append(", ").append(responses.get(i));
        }
        strBuilder.append("]}");
        return strBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void refreshStats(long timeout) throws HyracksDataException {
        LOGGER.log(Level.FINE, "refreshStats called");
        synchronized (this) {
            if (state != ActivityState.RUNNING || isFetchingStats) {
                LOGGER.log(Level.FINE,
                        "returning immediately since state = " + state + " and fetchingStats = " + isFetchingStats);
                return;
            } else {
                isFetchingStats = true;
            }
        }
        ICCMessageBroker messageBroker =
                (ICCMessageBroker) metadataProvider.getApplicationContext().getServiceContext().getMessageBroker();
        long reqId = messageBroker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = Arrays.asList(locations.getLocations());
        for (int i = 0; i < ncs.size(); i++) {
            requests.add(new StatsRequestMessage(ActiveManagerMessage.REQUEST_STATS,
                    new ActiveRuntimeId(entityId, runtimeName, i), reqId));
        }
        try {
            List<String> responses = (List<String>) messageBroker.sendSyncRequestToNCs(reqId, ncs, requests, timeout);
            stats = formatStats(responses);
            statsTimestamp = System.currentTimeMillis();
            notifySubscribers(statsUpdatedEvent);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        isFetchingStats = false;
    }

    protected synchronized void notifySubscribers(ActiveEvent event) {
        notifyAll();
        Iterator<IActiveEntityEventSubscriber> it = subscribers.iterator();
        while (it.hasNext()) {
            IActiveEntityEventSubscriber subscriber = it.next();
            if (subscriber.isDone()) {
                it.remove();
            } else {
                try {
                    subscriber.notify(event);
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.WARNING, "Failed to notify subscriber", e);
                }
                if (subscriber.isDone()) {
                    it.remove();
                }
            }
        }
    }

    public AlgebricksAbsolutePartitionConstraint getLocations() {
        return locations;
    }

    /**
     * this method is called whenever an action is requested. It ensures no interleaved requests
     *
     * @throws InterruptedException
     */
    protected synchronized void waitForNonTransitionState() throws InterruptedException {
        while (TRANSITION_STATES.contains(state) || suspended) {
            this.wait();
        }
    }

    /**
     * this method is called before an action call is returned. It ensures that the request didn't fail
     *
     */
    protected synchronized void checkNoFailure() throws HyracksDataException {
        if (state == ActivityState.PERMANENTLY_FAILED) {
            throw HyracksDataException.create(jobFailure);
        }
    }

    @Override
    public synchronized void recover() throws HyracksDataException {
        LOGGER.log(Level.FINE, "Recover is called on " + entityId);
        if (recoveryTask != null) {
            LOGGER.log(Level.FINE, "But recovery task for " + entityId + " is already there!! throwing an exception");
            throw new RuntimeDataException(ErrorCode.DOUBLE_RECOVERY_ATTEMPTS);
        }
        if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
            LOGGER.log(Level.FINE, "But it has no recovery policy, so it is set to permanent failure");
            setState(ActivityState.PERMANENTLY_FAILED);
        } else {
            ExecutorService executor = appCtx.getServiceContext().getControllerService().getExecutor();
            IRetryPolicy policy = retryPolicyFactory.create(this);
            cancelRecovery = false;
            setState(ActivityState.TEMPORARILY_FAILED);
            LOGGER.log(Level.FINE, "Recovery task has been submitted");
            recoveryTask = executor.submit(() -> doRecover(policy));
        }
    }

    protected Void doRecover(IRetryPolicy policy)
            throws AlgebricksException, HyracksDataException, InterruptedException {
        LOGGER.log(Level.FINE, "Actual Recovery task has started");
        if (getState() != ActivityState.TEMPORARILY_FAILED) {
            LOGGER.log(Level.FINE, "but its state is not temp failure and so we're just returning");
            return null;
        }
        LOGGER.log(Level.FINE, "calling the policy");
        while (policy.retry()) {
            synchronized (this) {
                if (cancelRecovery) {
                    recoveryTask = null;
                    return null;
                }
                while (clusterStateManager.getState() != ClusterState.ACTIVE) {
                    if (cancelRecovery) {
                        recoveryTask = null;
                        return null;
                    }
                    wait();
                }
            }
            waitForNonTransitionState();
            IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
            lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(),
                    entityId.getDataverse() + '.' + entityId.getEntityName());
            for (Dataset dataset : getDatasets()) {
                MetadataLockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(),
                        dataset.getDataverseName(), DatasetUtil.getFullyQualifiedName(dataset));
            }
            synchronized (this) {
                try {
                    setState(ActivityState.RECOVERING);
                    doStart(metadataProvider);
                    return null;
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Attempt to revive " + entityId + " failed", e);
                    setState(ActivityState.TEMPORARILY_FAILED);
                    recoverFailure = e;
                } finally {
                    metadataProvider.getLocks().reset();
                }
                notifyAll();
            }
        }
        IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
        try {
            lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(),
                    entityId.getDataverse() + '.' + entityId.getEntityName());
            for (Dataset dataset : getDatasets()) {
                MetadataLockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(), dataset.getDatasetName(),
                        DatasetUtil.getFullyQualifiedName(dataset));
            }
            synchronized (this) {
                if (state == ActivityState.TEMPORARILY_FAILED) {
                    setState(ActivityState.PERMANENTLY_FAILED);
                    recoveryTask = null;
                }
                notifyAll();
            }
        } finally {
            metadataProvider.getLocks().reset();
        }
        return null;
    }

    @Override
    public synchronized void start(MetadataProvider metadataProvider)
            throws HyracksDataException, InterruptedException {
        waitForNonTransitionState();
        if (state != ActivityState.PERMANENTLY_FAILED && state != ActivityState.STOPPED) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED, entityId, state);
        }
        try {
            setState(ActivityState.STARTING);
            doStart(metadataProvider);
            setRunning(metadataProvider, true);
        } catch (Exception e) {
            setState(ActivityState.PERMANENTLY_FAILED);
            LOGGER.log(Level.SEVERE, "Failed to start the entity " + entityId, e);
            throw HyracksDataException.create(e);
        }
    }

    protected abstract void doStart(MetadataProvider metadataProvider) throws HyracksDataException, AlgebricksException;

    protected abstract Void doStop(MetadataProvider metadataProvider) throws HyracksDataException, AlgebricksException;

    protected abstract Void doSuspend(MetadataProvider metadataProvider)
            throws HyracksDataException, AlgebricksException;

    protected abstract void doResume(MetadataProvider metadataProvider)
            throws HyracksDataException, AlgebricksException;

    protected abstract void setRunning(MetadataProvider metadataProvider, boolean running)
            throws HyracksDataException, AlgebricksException;

    @Override
    public void stop(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException {
        Future<Void> aRecoveryTask = null;
        synchronized (this) {
            waitForNonTransitionState();
            if (state != ActivityState.RUNNING && state != ActivityState.PERMANENTLY_FAILED
                    && state != ActivityState.TEMPORARILY_FAILED) {
                throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED, entityId, state);
            }
            if (state == ActivityState.TEMPORARILY_FAILED || state == ActivityState.PERMANENTLY_FAILED) {
                if (recoveryTask != null) {
                    aRecoveryTask = recoveryTask;
                    cancelRecovery = true;
                    recoveryTask.cancel(true);
                }
                setState(ActivityState.STOPPED);
                try {
                    setRunning(metadataProvider, false);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to set the entity state as not running " + entityId, e);
                    throw HyracksDataException.create(e);
                }
            } else if (state == ActivityState.RUNNING) {
                setState(ActivityState.STOPPING);
                try {
                    doStop(metadataProvider);
                    setRunning(metadataProvider, false);
                } catch (Exception e) {
                    setState(ActivityState.PERMANENTLY_FAILED);
                    LOGGER.log(Level.SEVERE, "Failed to stop the entity " + entityId, e);
                    throw HyracksDataException.create(e);
                }
            } else {
                throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED, entityId, state);
            }
        }
        try {
            if (aRecoveryTask != null) {
                aRecoveryTask.get();
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void suspend(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException {
        WaitForStateSubscriber subscriber;
        Future<Void> suspendTask;
        synchronized (this) {
            LOGGER.log(Level.FINE, "suspending entity " + entityId);
            LOGGER.log(Level.FINE, "Waiting for ongoing activities");
            waitForNonTransitionState();
            LOGGER.log(Level.FINE, "Proceeding with suspension. Current state is " + state);
            if (state == ActivityState.STOPPED || state == ActivityState.PERMANENTLY_FAILED) {
                suspended = true;
                return;
            }
            if (state == ActivityState.SUSPENDED) {
                throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_ALREADY_SUSPENDED, entityId, state);
            }
            if (state == ActivityState.TEMPORARILY_FAILED) {
                suspended = true;
                setState(ActivityState.SUSPENDED);
                return;
            }
            setState(ActivityState.SUSPENDING);
            subscriber = new WaitForStateSubscriber(this,
                    EnumSet.of(ActivityState.SUSPENDED, ActivityState.TEMPORARILY_FAILED));
            suspendTask = metadataProvider.getApplicationContext().getServiceContext().getControllerService()
                    .getExecutor().submit(() -> doSuspend(metadataProvider));
            LOGGER.log(Level.FINE, "Suspension task has been submitted");
        }
        try {
            LOGGER.log(Level.FINE, "Waiting for suspension task to complete");
            suspendTask.get();
            LOGGER.log(Level.FINE, "waiting for state to become SUSPENDED or TEMPORARILY_FAILED");
            subscriber.sync();
        } catch (Exception e) {
            synchronized (this) {
                LOGGER.log(Level.SEVERE, "Failure while waiting for " + entityId + " to become suspended", e);
                // failed to suspend
                if (state == ActivityState.SUSPENDING) {
                    if (jobId != null) {
                        // job is still running
                        // restore state
                        setState(prevState);
                    } else {
                        setState(ActivityState.PERMANENTLY_FAILED);
                    }
                }
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public synchronized void resume(MetadataProvider metadataProvider) throws HyracksDataException {
        if (state == ActivityState.STOPPED || state == ActivityState.PERMANENTLY_FAILED) {
            suspended = false;
            notifyAll();
            return;
        }
        if (state != ActivityState.SUSPENDED && state != ActivityState.TEMPORARILY_FAILED) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_CANNOT_RESUME_FROM_STATE, entityId, state);
        }
        try {
            if (prevState == ActivityState.TEMPORARILY_FAILED) {
                setState(ActivityState.TEMPORARILY_FAILED);
                return;
            }
            setState(ActivityState.RESUMING);
            WaitForStateSubscriber subscriber = new WaitForStateSubscriber(this,
                    EnumSet.of(ActivityState.RUNNING, ActivityState.TEMPORARILY_FAILED));
            recoveryTask = metadataProvider.getApplicationContext().getServiceContext().getControllerService()
                    .getExecutor().submit(() -> resumeOrRecover(metadataProvider));
            try {
                subscriber.sync();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failure while attempting to resume " + entityId, e);
                throw HyracksDataException.create(e);
            }
        } finally {
            suspended = false;
            notifyAll();
        }
    }

    protected Void resumeOrRecover(MetadataProvider metadataProvider)
            throws HyracksDataException, AlgebricksException, InterruptedException {
        try {
            doResume(metadataProvider);
            synchronized (this) {
                setState(ActivityState.RUNNING);
                recoveryTask = null;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "First attempt to resume " + entityId + " Failed", e);
            setState(ActivityState.TEMPORARILY_FAILED);
            if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
                setState(ActivityState.PERMANENTLY_FAILED);
            } else {
                IRetryPolicy policy = retryPolicyFactory.create(this);
                cancelRecovery = false;
                doRecover(policy);
            }
        }
        return null;
    }

    @Override
    public boolean isActive() {
        return state != ActivityState.STOPPED && state != ActivityState.PERMANENTLY_FAILED;
    }

    @Override
    public void unregister() throws HyracksDataException {
        handler.unregisterListener(this);
    }

    public void setLocations(AlgebricksAbsolutePartitionConstraint locations) {
        this.locations = locations;
    }

    public Future<Void> getRecoveryTask() {
        return recoveryTask;
    }

    public synchronized void cancelRecovery() {
        cancelRecovery = true;
        notifyAll();
    }

    @Override
    public Exception getJobFailure() {
        return jobFailure;
    }

    @Override
    public List<Dataset> getDatasets() {
        return datasets;
    }

    @Override
    public synchronized void replace(Dataset dataset) {
        if (getDatasets().contains(dataset)) {
            getDatasets().remove(dataset);
            getDatasets().add(dataset);
        }
    }
}
