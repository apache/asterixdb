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

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveNotificationHandler;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
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
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActiveNotificationHandler extends SingleThreadEventProcessor<ActiveEvent>
        implements IActiveNotificationHandler, IJobLifecycleListener {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level level = Level.DEBUG;
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
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "Next event is of type " + event.getEventKind());
            }
            if (event.getEventKind() == Kind.JOB_FINISHED) {
                LOGGER.log(level, "Removing the job");
                jobId2EntityId.remove(event.getJobId());
            }
            if (listener != null) {
                LOGGER.log(level, "Notifying the listener");
                listener.notify(event);
            }
        } else {
            LOGGER.log(Level.ERROR, "Entity not found for received message for job " + event.getJobId());
        }
    }

    // *** IJobLifecycleListener

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) throws HyracksDataException {
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level,
                    "notifyJobCreation(JobId jobId, JobSpecification jobSpecification) was called with jobId = "
                            + jobId);
        }
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (!(property instanceof EntityId)) {
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "Job is not of type active job. property found to be: " + property);
            }
            return;
        }
        EntityId entityId = (EntityId) property;
        monitorJob(jobId, entityId);
        boolean found = jobId2EntityId.get(jobId) != null;
        LOGGER.log(level, "Job was found to be: " + (found ? "Active" : "Inactive"));
        add(new ActiveEvent(jobId, Kind.JOB_CREATED, entityId, jobSpecification));
    }

    private synchronized void monitorJob(JobId jobId, EntityId entityId) {
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "monitorJob(JobId jobId, ActiveJob activeJob) called with job id: " + jobId);
        }
        boolean found = jobId2EntityId.get(jobId) != null;
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        if (entityEventListeners.containsKey(entityId)) {
            if (jobId2EntityId.containsKey(jobId)) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Job is already being monitored for job: " + jobId);
                }
                return;
            }
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "monitoring started for job id: " + jobId);
            }
        } else {
            if (LOGGER.isEnabled(level)) {
                LOGGER.info("No listener was found for the entity: " + entityId);
            }
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
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "Getting notified of job finish for JobId: " + jobId);
        }
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            add(new ActiveEvent(jobId, Kind.JOB_FINISHED, entityId, Pair.of(jobStatus, exceptions)));
        } else {
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "no need to notify job finish");
            }
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
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "getActiveEntityListener(EntityId entityId) was called with entity " + entityId);
        }
        IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "Listener found: " + listener);
        }
        return entityEventListeners.get(entityId);
    }

    @Override
    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("getEventListeners() was called");
            LOGGER.trace("returning " + entityEventListeners.size() + " Listeners");
        }
        return entityEventListeners.values().toArray(new IActiveEntityEventsListener[entityEventListeners.size()]);
    }

    @Override
    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (suspended) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED);
        }
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "registerListener(IActiveEntityEventsListener listener) was called for the entity "
                    + listener.getEntityId());
        }
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
        if (LOGGER.isEnabled(level)) {
            LOGGER.log(level, "unregisterListener(IActiveEntityEventsListener listener) was called for the entity "
                    + listener.getEntityId());
        }
        IActiveEntityEventsListener registeredListener = entityEventListeners.remove(listener.getEntityId());
        if (registeredListener == null) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_ENTITY_LISTENER_IS_NOT_REGISTERED, listener.getEntityId());
        }
        if (registeredListener.isActive() && !registeredListener.isSuspended()) {
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
    public void recover() {
        LOGGER.info("Starting active recovery");
        for (IActiveEntityEventsListener listener : getEventListeners()) {
            synchronized (listener) {
                if (LOGGER.isEnabled(level)) {
                    LOGGER.log(level, "Entity " + listener.getEntityId() + " is " + listener.getState());
                }
                listener.notifyAll();
            }
        }
    }

    public void suspend(MetadataProvider mdProvider) throws HyracksDataException {
        synchronized (this) {
            if (suspended) {
                throw new RuntimeDataException(ErrorCode.ACTIVE_EVENT_HANDLER_ALREADY_SUSPENDED);
            }
            LOGGER.log(level, "Suspending active events handler");
            suspended = true;
        }
        Collection<IActiveEntityEventsListener> registeredListeners = entityEventListeners.values();
        for (IActiveEntityEventsListener listener : registeredListeners) {
            suspendForDdlOrHalt(listener, mdProvider, null);
        }
    }

    public void resume(MetadataProvider mdProvider) {
        LOGGER.log(level, "Resuming active events handler");
        for (IActiveEntityEventsListener listener : entityEventListeners.values()) {
            resumeOrHalt(listener, mdProvider);
        }
        synchronized (this) {
            suspended = false;
        }
    }

    public void suspendForDdlOrHalt(IActiveEntityEventsListener listener, MetadataProvider metadataProvider,
            Dataset targetDataset) {
        try {
            // write lock the listener
            // exclusive lock all the datasets (except the target dataset)
            IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
            String dataverseName = listener.getEntityId().getDataverse();
            String entityName = listener.getEntityId().getEntityName();
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "Suspending " + listener.getEntityId());
            }
            LOGGER.log(level, "Acquiring locks");
            lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(), dataverseName + '.' + entityName);
            List<Dataset> datasets = ((ActiveEntityEventsListener) listener).getDatasets();
            for (Dataset dataset : datasets) {
                if (targetDataset != null && targetDataset.equals(dataset)) {
                    // DDL operation already acquired the proper lock for the operation
                    continue;
                }
                lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(),
                        DatasetUtil.getFullyQualifiedName(dataset));
            }
            LOGGER.log(level, "locks acquired");
            ((ActiveEntityEventsListener) listener).suspend(metadataProvider);
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, listener.getEntityId() + " suspended");
            }
        } catch (Throwable th) { // NOSONAR must halt in case of any failure
            LOGGER.error("Suspend active failed", th);
            ExitUtil.halt(ExitUtil.EC_ACTIVE_SUSPEND_FAILURE);
        }
    }

    public void resumeOrHalt(IActiveEntityEventsListener listener, MetadataProvider metadataProvider) {
        try {
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "Resuming " + listener.getEntityId());
            }
            ((ActiveEntityEventsListener) listener).resume(metadataProvider);
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, listener.getEntityId() + " resumed");
            }
        } catch (Throwable th) { // NOSONAR must halt in case of any failure
            LOGGER.error("Resume active failed", th);
            ExitUtil.halt(ExitUtil.EC_ACTIVE_RESUME_FAILURE);
        }
    }
}
