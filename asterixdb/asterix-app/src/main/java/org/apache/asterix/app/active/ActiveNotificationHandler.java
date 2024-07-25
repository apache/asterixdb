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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveNotificationHandler;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
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
    private boolean suspended = false;

    public ActiveNotificationHandler() {
        super(ActiveNotificationHandler.class.getSimpleName());
        jobId2EntityId = new HashMap<>();
        entityEventListeners = new HashMap<>();
    }

    // *** SingleThreadEventProcessor<ActiveEvent>

    @Override
    protected void handle(ActiveEvent event) {
        JobId jobId = event.getJobId();
        Kind eventKind = event.getEventKind();
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
            if (eventKind == Kind.JOB_FINISHED) {
                LOGGER.debug("removing ingestion job {}", jobId);
                jobId2EntityId.remove(jobId);
            }
            if (listener != null) {
                listener.notify(event);
            } else {
                LOGGER.debug("listener not found for entity {} on event={}", entityId, event);
            }
        } else {
            LOGGER.error("entity not found for event {}", event);
        }
    }

    // *** IJobLifecycleListener

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) throws HyracksDataException {
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (!(property instanceof EntityId)) {
            if (property != null) {
                LOGGER.debug("{} is not an active job. job property={}", jobId, property);
            }
            return;
        }
        LOGGER.debug("notified of ingestion job creation {}", jobId);
        EntityId entityId = (EntityId) property;
        monitorJob(jobId, entityId);
        add(new ActiveEvent(jobId, Kind.JOB_CREATED, entityId, jobSpecification));
    }

    private synchronized void monitorJob(JobId jobId, EntityId entityId) {
        boolean found = jobId2EntityId.containsKey(jobId);
        LOGGER.debug("{} is {}", jobId, (found ? "active" : "inactive"));
        if (entityEventListeners.containsKey(entityId)) {
            if (found) {
                LOGGER.error("{} is already being monitored", jobId);
                return;
            }
            LOGGER.debug("monitoring started for {}", jobId);
        } else {
            LOGGER.debug("no listener found for entity {}; {}", entityId, jobId);
        }
        jobId2EntityId.put(jobId, entityId);
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            add(new ActiveEvent(jobId, Kind.JOB_STARTED, entityId, null));
        }
        // else must be non-active job, e.g. a job for a query
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions)
            throws HyracksException {
        EntityId entityId = jobId2EntityId.get(jobId);
        if (entityId != null) {
            LOGGER.debug("notified of ingestion job finish {}", jobId);
            add(new ActiveEvent(jobId, Kind.JOB_FINISHED, entityId, Pair.of(jobStatus, exceptions)));
        }
        // else must be non-active job, e.g. a job for a query
    }

    // *** IActiveNotificationHandler

    @Override
    public void receive(ActivePartitionMessage message) {
        add(new ActiveEvent(message.getJobId(), Kind.PARTITION_EVENT, message.getActiveRuntimeId().getEntityId(),
                message));
    }

    @Override
    public IActiveEntityEventsListener getListener(EntityId entityId) {
        return entityEventListeners.get(entityId);
    }

    @Override
    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("getEventListeners() returning {} listeners", entityEventListeners.size());
        }
        return entityEventListeners.values().toArray(IActiveEntityEventsListener[]::new);
    }

    @Override
    public synchronized Collection<IActiveEntityEventsListener> getEventListenersAsList() {
        return Collections.unmodifiableCollection(entityEventListeners.values());
    }

    @Override
    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (suspended) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED);
        }
        LOGGER.debug("register listener for entity {}, state={}", listener.getEntityId(), listener.getState());
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
        LOGGER.debug("unregister listener for entity {}, state={}", listener.getEntityId(), listener.getState());
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
    public void recover() {
        LOGGER.info("Starting active recovery");
        for (IActiveEntityEventsListener listener : getEventListeners()) {
            synchronized (listener) {
                LOGGER.debug("entity {} is {}, active={}, suspended={}", listener.getEntityId(), listener.getState(),
                        listener.isActive(), listener.isSuspended());
                listener.notifyAll();
            }
        }
    }

    public void suspend(MetadataProvider mdProvider, String reason) throws HyracksDataException {
        synchronized (this) {
            if (suspended) {
                throw new RuntimeDataException(ErrorCode.ACTIVE_EVENT_HANDLER_ALREADY_SUSPENDED);
            }
            LOGGER.debug("suspending active events handler. reason {}", reason);
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
            EntityId entityId = listener.getEntityId();
            LOGGER.log(level, "Suspending {}", entityId);
            LOGGER.log(level, "Acquiring locks for {}", entityId);
            ((ActiveEntityEventsListener) listener).acquireSuspendLocks(metadataProvider, targetDataset);
            LOGGER.log(level, "locks acquired for {}", entityId);
            ((ActiveEntityEventsListener) listener).suspend(metadataProvider);
            LOGGER.log(level, "{} suspended", entityId);
        } catch (Throwable th) { // NOSONAR must halt in case of any failure
            LOGGER.error("Suspend active failed", th);
            ExitUtil.halt(ExitUtil.EC_ACTIVE_SUSPEND_FAILURE);
        }
    }

    public void resumeOrHalt(IActiveEntityEventsListener listener, MetadataProvider metadataProvider) {
        try {
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "Resuming {}", listener.getEntityId());
            }
            ((ActiveEntityEventsListener) listener).resume(metadataProvider);
            if (LOGGER.isEnabled(level)) {
                LOGGER.log(level, "{} resumed", listener.getEntityId());
            }
        } catch (Throwable th) { // NOSONAR must halt in case of any failure
            LOGGER.error("Resume active failed", th);
            ExitUtil.halt(ExitUtil.EC_ACTIVE_RESUME_FAILURE);
        }
    }
}
