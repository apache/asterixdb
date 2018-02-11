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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.ActivePartitionMessage.Event;
import org.apache.asterix.active.message.ActiveStatsRequestMessage;
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
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ActiveEntityEventsListener implements IActiveEntityController {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level level = Level.INFO;
    private static final ActiveEvent STATE_CHANGED = new ActiveEvent(null, Kind.STATE_CHANGED, null, null);
    private static final EnumSet<ActivityState> TRANSITION_STATES = EnumSet.of(ActivityState.RESUMING,
            ActivityState.STARTING, ActivityState.STOPPING, ActivityState.RECOVERING);
    private static final String DEFAULT_ACTIVE_STATS = "{\"Stats\":\"N/A\"}";
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
    protected int numDeRegistered;
    protected volatile RecoveryTask rt;
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
        this.hcc = hcc;
        this.entityId = entityId;
        this.datasets = datasets;
        this.retryPolicyFactory = retryPolicyFactory;
        this.state = ActivityState.STOPPED;
        this.statsTimestamp = -1;
        this.isFetchingStats = false;
        this.statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId, null);
        this.stats = DEFAULT_ACTIVE_STATS;
        this.runtimeName = runtimeName;
        this.locations = locations;
        this.numRegistered = 0;
        this.numDeRegistered = 0;
        this.handler =
                (ActiveNotificationHandler) metadataProvider.getApplicationContext().getActiveNotificationHandler();
        handler.registerListener(this);
    }

    protected synchronized void setState(ActivityState newState) {
        LOGGER.log(level, "State of " + getEntityId() + "is being set to " + newState + " from " + state);
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
            LOGGER.log(level, "EventListener is notified.");
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
                    LOGGER.log(Level.DEBUG, "Unhandled feed event notification: " + event);
                    break;
            }
            notifySubscribers(event);
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Unhandled Exception", e);
        }
    }

    protected void jobCreated(ActiveEvent event) {
        // Do nothing
    }

    protected synchronized void handle(ActivePartitionMessage message) {
        if (message.getEvent() == Event.RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == locations.getLocations().length) {
                setState(ActivityState.RUNNING);
            }
        } else if (message.getEvent() == Event.RUNTIME_DEREGISTERED) {
            numDeRegistered++;
        }
    }

    @SuppressWarnings("unchecked")
    protected void finish(ActiveEvent event) throws HyracksDataException {
        LOGGER.log(level, "the job " + jobId + " finished");
        if (numRegistered != numDeRegistered) {
            LOGGER.log(Level.WARN, "the job " + jobId + " finished with reported runtime registrations = "
                    + numRegistered + " and deregistrations = " + numDeRegistered + " on node controllers");
        }
        jobId = null;
        Pair<JobStatus, List<Exception>> status = (Pair<JobStatus, List<Exception>>) event.getEventObject();
        JobStatus jobStatus = status.getLeft();
        List<Exception> exceptions = status.getRight();
        LOGGER.log(level, "The job finished with status: " + jobStatus);
        if (jobStatus.equals(JobStatus.FAILURE)) {
            jobFailure = exceptions.isEmpty() ? new RuntimeDataException(ErrorCode.UNREPORTED_TASK_FAILURE_EXCEPTION)
                    : exceptions.get(0);
            setState((state == ActivityState.STOPPING) ? ActivityState.STOPPED : ActivityState.TEMPORARILY_FAILED);
            if (prevState != ActivityState.SUSPENDING && prevState != ActivityState.RECOVERING
                    && prevState != ActivityState.RESUMING && prevState != ActivityState.STOPPING) {
                recover();
            }
        } else {
            setState(state == ActivityState.SUSPENDING ? ActivityState.SUSPENDED : ActivityState.STOPPED);
        }
    }

    protected void start(ActiveEvent event) {
        jobId = event.getJobId();
        numRegistered = 0;
        numDeRegistered = 0;
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
        LOGGER.log(level, "refreshStats called");
        synchronized (this) {
            if (state != ActivityState.RUNNING) {
                LOGGER.log(level, "returning immediately since state = " + state);
                notifySubscribers(statsUpdatedEvent);
                return;
            } else if (isFetchingStats) {
                LOGGER.log(level, "returning immediately since fetchingStats = " + isFetchingStats);
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
            requests.add(new ActiveStatsRequestMessage(new ActiveRuntimeId(entityId, runtimeName, i), reqId));
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
                    LOGGER.log(Level.WARN, "Failed to notify subscriber", e);
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
    public synchronized void recover() {
        LOGGER.log(level, "Recover is called on " + entityId);
        if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
            LOGGER.log(level, "But it has no recovery policy, so it is set to permanent failure");
            setState(ActivityState.PERMANENTLY_FAILED);
        } else {
            ExecutorService executor = appCtx.getServiceContext().getControllerService().getExecutor();
            setState(ActivityState.TEMPORARILY_FAILED);
            LOGGER.log(level, "Recovery task has been submitted");
            rt = new RecoveryTask(appCtx, this, retryPolicyFactory);
            executor.submit(rt.recover());
        }
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
            LOGGER.log(Level.ERROR, "Failed to start the entity " + entityId, e);
            throw HyracksDataException.create(e);
        }
    }

    protected abstract void doStart(MetadataProvider metadataProvider) throws HyracksDataException;

    protected abstract Void doStop(MetadataProvider metadataProvider) throws HyracksDataException;

    protected abstract Void doSuspend(MetadataProvider metadataProvider) throws HyracksDataException;

    protected abstract void doResume(MetadataProvider metadataProvider) throws HyracksDataException;

    protected abstract void setRunning(MetadataProvider metadataProvider, boolean running) throws HyracksDataException;

    @Override
    public synchronized void stop(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException {
        waitForNonTransitionState();
        if (state != ActivityState.RUNNING && state != ActivityState.PERMANENTLY_FAILED
                && state != ActivityState.TEMPORARILY_FAILED) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED, entityId, state);
        }
        if (state == ActivityState.TEMPORARILY_FAILED || state == ActivityState.PERMANENTLY_FAILED) {
            if (rt != null) {
                setState(ActivityState.STOPPING);
                rt.cancel();
                rt = null;
            }
            setState(ActivityState.STOPPED);
            try {
                setRunning(metadataProvider, false);
            } catch (Exception e) {
                LOGGER.log(Level.ERROR, "Failed to set the entity state as not running " + entityId, e);
                throw HyracksDataException.create(e);
            }
        } else if (state == ActivityState.RUNNING) {
            setState(ActivityState.STOPPING);
            try {
                doStop(metadataProvider);
                setRunning(metadataProvider, false);
            } catch (Exception e) {
                setState(ActivityState.PERMANENTLY_FAILED);
                LOGGER.log(Level.ERROR, "Failed to stop the entity " + entityId, e);
                throw HyracksDataException.create(e);
            }
        } else {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED, entityId, state);
        }
        this.stats = DEFAULT_ACTIVE_STATS;
        notifySubscribers(statsUpdatedEvent);
    }

    public RecoveryTask getRecoveryTask() {
        return rt;
    }

    @Override
    public void suspend(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException {
        WaitForStateSubscriber subscriber;
        Future<Void> suspendTask;
        synchronized (this) {
            LOGGER.log(level, "suspending entity " + entityId);
            LOGGER.log(level, "Waiting for ongoing activities");
            waitForNonTransitionState();
            LOGGER.log(level, "Proceeding with suspension. Current state is " + state);
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
            LOGGER.log(level, "Suspension task has been submitted");
        }
        try {
            LOGGER.log(level, "Waiting for suspension task to complete");
            suspendTask.get();
            LOGGER.log(level, "waiting for state to become SUSPENDED or TEMPORARILY_FAILED");
            subscriber.sync();
        } catch (Exception e) {
            synchronized (this) {
                LOGGER.log(Level.ERROR, "Failure while waiting for " + entityId + " to become suspended", e);
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
            rt = new RecoveryTask(appCtx, this, retryPolicyFactory);
            try {
                rt.resumeOrRecover(metadataProvider);
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Failure while attempting to resume " + entityId, e);
            }
        } finally {
            suspended = false;
            notifyAll();
        }
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

    @Override
    public String getDisplayName() throws HyracksDataException {
        return this.getEntityId().toString();
    }

    @Override
    public String toString() {
        return "{\"class\":\"" + getClass().getSimpleName() + "\"" + "\"entityId\":\"" + entityId + "\""
                + "\"state\":\"" + state + "\"" + "}";
    }
}
