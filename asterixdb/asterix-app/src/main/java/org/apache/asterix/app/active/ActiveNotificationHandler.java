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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveNotificationHandler;
import org.apache.asterix.active.SingleThreadEventProcessor;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.metadata.api.IActiveEntityController;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

public class ActiveNotificationHandler extends SingleThreadEventProcessor<ActiveEvent>
        implements IActiveNotificationHandler, IJobLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveNotificationHandler.class.getName());
    private static final Level level = Level.INFO;
    public static final String ACTIVE_ENTITY_PROPERTY_NAME = "ActiveJob";
    private final Map<EntityId, IActiveEntityEventsListener> entityEventListeners;
    private final Map<JobId, EntityId> jobId2EntityId;
    private boolean initialized = false;
    private boolean suspended = false;

    public ActiveNotificationHandler() {
        super(ActiveNotificationHandler.class.getSimpleName());
        jobId2EntityId = new HashMap<>();
        entityEventListeners = new HashMap<>();
    }

    // *** SingleThreadEventProcessor<ActiveEvent>

    @Override
    protected void handle(ActiveEvent event) {
        EntityId entityId = jobId2EntityId.get(event.getJobId());
        if (entityId != null) {
            IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
            LOGGER.log(level, "Next event is of type " + event.getEventKind());
            if (event.getEventKind() == Kind.JOB_FINISHED) {
                LOGGER.log(level, "Removing the job");
                jobId2EntityId.remove(event.getJobId());
            }
            if (listener != null) {
                LOGGER.log(level, "Notifying the listener");
                listener.notify(event);
            }
        } else {
            LOGGER.log(Level.SEVERE, "Entity not found for received message for job " + event.getJobId());
        }
    }

    // *** IJobLifecycleListener

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) throws HyracksDataException {
        LOGGER.log(level,
                "notifyJobCreation(JobId jobId, JobSpecification jobSpecification) was called with jobId = " + jobId);
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (property == null || !(property instanceof EntityId)) {
            LOGGER.log(level, "Job is not of type active job. property found to be: " + property);
            return;
        }
        EntityId entityId = (EntityId) property;
        monitorJob(jobId, entityId);
        boolean found = jobId2EntityId.get(jobId) != null;
        LOGGER.log(level, "Job was found to be: " + (found ? "Active" : "Inactive"));
        add(new ActiveEvent(jobId, Kind.JOB_CREATED, entityId, jobSpecification));
    }

    private synchronized void monitorJob(JobId jobId, EntityId entityId) {
        LOGGER.log(level, "monitorJob(JobId jobId, ActiveJob activeJob) called with job id: " + jobId);
        boolean found = jobId2EntityId.get(jobId) != null;
        LOGGER.log(level, "Job was found to be: " + (found ? "Active" : "Inactive"));
        if (entityEventListeners.containsKey(entityId)) {
            if (jobId2EntityId.containsKey(jobId)) {
                LOGGER.severe("Job is already being monitored for job: " + jobId);
                return;
            }
            LOGGER.log(level, "monitoring started for job id: " + jobId);
        } else {
            LOGGER.info("No listener was found for the entity: " + entityId);
        }
        jobId2EntityId.put(jobId, entityId);
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            add(new ActiveEvent(jobId, Kind.JOB_STARTED, entityId, null));
        }
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions)
            throws HyracksException {
        LOGGER.log(level, "Getting notified of job finish for JobId: " + jobId);
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            add(new ActiveEvent(jobId, Kind.JOB_FINISHED, entityId, Pair.of(jobStatus, exceptions)));
        } else {
            LOGGER.log(level, "NO NEED TO NOTIFY JOB FINISH!");
        }
    }

    // *** IActiveNotificationHandler

    @Override
    public void receive(ActivePartitionMessage message) {
        add(new ActiveEvent(message.getJobId(), Kind.PARTITION_EVENT, message.getActiveRuntimeId().getEntityId(),
                message));
    }

    @Override
    public IActiveEntityEventsListener getListener(EntityId entityId) {
        LOGGER.log(level, "getActiveEntityListener(EntityId entityId) was called with entity " + entityId);
        IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
        LOGGER.log(level, "Listener found: " + listener);
        return entityEventListeners.get(entityId);
    }

    @Override
    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        LOGGER.log(level, "getEventListeners() was called");
        LOGGER.log(level, "returning " + entityEventListeners.size() + " Listeners");
        return entityEventListeners.values().toArray(new IActiveEntityEventsListener[entityEventListeners.size()]);
    }

    @Override
    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (suspended) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED);
        }
        LOGGER.log(level, "registerListener(IActiveEntityEventsListener listener) was called for the entity "
                + listener.getEntityId());
        if (entityEventListeners.containsKey(listener.getEntityId())) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_IS_ALREADY_REGISTERED, listener.getEntityId());
        }
        entityEventListeners.put(listener.getEntityId(), listener);
    }

    @Override
    public synchronized void unregisterListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (suspended) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED);
        }
        LOGGER.log(level, "unregisterListener(IActiveEntityEventsListener listener) was called for the entity "
                + listener.getEntityId());
        IActiveEntityEventsListener registeredListener = entityEventListeners.remove(listener.getEntityId());
        if (registeredListener == null) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_LISTENER_IS_NOT_REGISTERED, listener.getEntityId());
        }
        if (registeredListener.isActive()) {
            entityEventListeners.put(registeredListener.getEntityId(), registeredListener);
            throw new RuntimeDataException(ErrorCode.CANNOT_DERIGESTER_ACTIVE_ENTITY_LISTENER, listener.getEntityId());
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void setInitialized(boolean initialized) throws HyracksDataException {
        if (this.initialized) {
            throw new RuntimeDataException(ErrorCode.DOUBLE_INITIALIZATION_OF_ACTIVE_NOTIFICATION_HANDLER);
        }
        this.initialized = initialized;
    }

    @Override
    public synchronized void recover() throws HyracksDataException {
        LOGGER.log(level, "Starting active recovery");
        for (IActiveEntityEventsListener listener : entityEventListeners.values()) {
            synchronized (listener) {
                LOGGER.log(level, "Entity " + listener.getEntityId() + " is " + listener.getStats());
                if (listener.getState() == ActivityState.PERMANENTLY_FAILED
                        && listener instanceof IActiveEntityController) {
                    LOGGER.log(level, "Recovering");
                    ((IActiveEntityController) listener).recover();
                } else {
                    LOGGER.log(level, "Only notifying");
                    listener.notifyAll();
                }
            }
        }
    }

    public void suspend(MetadataProvider mdProvider)
            throws AsterixException, HyracksDataException, InterruptedException {
        synchronized (this) {
            if (suspended) {
                throw new RuntimeDataException(ErrorCode.ACTIVE_EVENT_HANDLER_ALREADY_SUSPENDED);
            }
            LOGGER.log(level, "Suspending active events handler");
            suspended = true;
        }
        IMetadataLockManager lockManager = mdProvider.getApplicationContext().getMetadataLockManager();
        Collection<IActiveEntityEventsListener> registeredListeners = entityEventListeners.values();
        for (IActiveEntityEventsListener listener : registeredListeners) {
            // write lock the listener
            // exclusive lock all the datasets
            String dataverseName = listener.getEntityId().getDataverse();
            String entityName = listener.getEntityId().getEntityName();
            LOGGER.log(level, "Suspending " + listener.getEntityId());
            LOGGER.log(level, "Acquiring locks");
            lockManager.acquireActiveEntityWriteLock(mdProvider.getLocks(), dataverseName + '.' + entityName);
            List<Dataset> datasets = ((ActiveEntityEventsListener) listener).getDatasets();
            for (Dataset dataset : datasets) {
                lockManager.acquireDatasetExclusiveModificationLock(mdProvider.getLocks(),
                        DatasetUtil.getFullyQualifiedName(dataset));
            }
            LOGGER.log(level, "locks acquired");
            ((ActiveEntityEventsListener) listener).suspend(mdProvider);
            LOGGER.log(level, listener.getEntityId() + " suspended");
        }
    }

    public void resume(MetadataProvider mdProvider)
            throws AsterixException, HyracksDataException, InterruptedException {
        LOGGER.log(level, "Resuming active events handler");
        for (IActiveEntityEventsListener listener : entityEventListeners.values()) {
            LOGGER.log(level, "Resuming " + listener.getEntityId());
            ((ActiveEntityEventsListener) listener).resume(mdProvider);
            LOGGER.log(level, listener.getEntityId() + " resumed");
        }
        synchronized (this) {
            suspended = false;
        }
    }
}
