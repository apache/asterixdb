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
package org.apache.asterix.active;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveJobNotificationHandler implements Runnable {
    public static final ActiveJobNotificationHandler INSTANCE = new ActiveJobNotificationHandler();
    public static final String ACTIVE_ENTITY_PROPERTY_NAME = "ActiveJob";
    private static final Logger LOGGER = Logger.getLogger(ActiveJobNotificationHandler.class.getName());
    private static final boolean DEBUG = false;
    private final LinkedBlockingQueue<ActiveEvent> eventInbox;
    private final Map<EntityId, IActiveEntityEventsListener> entityEventListeners;
    private final Map<JobId, EntityId> jobId2ActiveJobInfos;

    private ActiveJobNotificationHandler() {
        this.eventInbox = new LinkedBlockingQueue<>();
        this.jobId2ActiveJobInfos = new HashMap<>();
        this.entityEventListeners = new HashMap<>();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(ActiveJobNotificationHandler.class.getSimpleName());
        LOGGER.log(Level.INFO, "Started " + ActiveJobNotificationHandler.class.getSimpleName());
        while (!Thread.interrupted()) {
            try {
                ActiveEvent event = getEventInbox().take();
                EntityId entityId = jobId2ActiveJobInfos.get(event.getJobId());
                if (entityId != null) {
                    IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
                    LOGGER.log(Level.FINER, "Next event is of type " + event.getEventKind());
                    if (event.getEventKind() == Kind.JOB_FINISHED) {
                        LOGGER.log(Level.FINER, "Removing the job");
                        jobId2ActiveJobInfos.remove(event.getJobId());
                    }
                    if (listener != null) {
                        LOGGER.log(Level.FINER, "Notifying the listener");
                        listener.notify(event);
                    }

                } else {
                    LOGGER.log(Level.SEVERE, "Entity not found for received message for job " + event.getJobId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error handling an active job event", e);
            }
        }
        LOGGER.log(Level.INFO, "Stopped " + ActiveJobNotificationHandler.class.getSimpleName());
    }

    public synchronized void removeListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        LOGGER.log(Level.FINER, "Removing the listener since it is not active anymore");
        unregisterListener(listener);
    }

    public IActiveEntityEventsListener getActiveEntityListener(EntityId entityId) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getActiveEntityListener(EntityId entityId) was called with entity " + entityId);
            IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
            LOGGER.log(Level.WARNING, "Listener found: " + listener);
        }
        return entityEventListeners.get(entityId);
    }

    public EntityId getEntity(JobId jobId) {
        return jobId2ActiveJobInfos.get(jobId);
    }

    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) {
        LOGGER.log(Level.FINER,
                "notifyJobCreation(JobId jobId, JobSpecification jobSpecification) was called with jobId = " + jobId);
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (property == null || !(property instanceof EntityId)) {
            LOGGER.log(Level.FINER, "Job was is not active. property found to be: " + property);
            return;
        }
        EntityId entityId = (EntityId) property;
        monitorJob(jobId, entityId);
        boolean found = jobId2ActiveJobInfos.get(jobId) != null;
        LOGGER.log(Level.FINER, "Job was found to be: " + (found ? "Active" : "Inactive"));
        IActiveEntityEventsListener listener = entityEventListeners.get(entityId);
        if (listener != null) {
            listener.notify(new ActiveEvent(jobId, Kind.JOB_CREATED, entityId, jobSpecification));
        }
        LOGGER.log(Level.FINER, "Listener was notified" + jobId);
    }

    public LinkedBlockingQueue<ActiveEvent> getEventInbox() {
        return eventInbox;
    }

    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getEventListeners() was called");
            LOGGER.log(Level.WARNING, "returning " + entityEventListeners.size() + " Listeners");
        }
        return entityEventListeners.values().toArray(new IActiveEntityEventsListener[entityEventListeners.size()]);
    }

    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (DEBUG) {
            LOGGER.log(Level.FINER, "registerListener(IActiveEntityEventsListener listener) was called for the entity "
                    + listener.getEntityId());
        }
        if (entityEventListeners.containsKey(listener.getEntityId())) {
            throw new HyracksDataException(
                    "Active Entity Listener " + listener.getEntityId() + " is already registered");
        }
        entityEventListeners.put(listener.getEntityId(), listener);
    }

    public synchronized void unregisterListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        LOGGER.log(Level.FINER, "unregisterListener(IActiveEntityEventsListener listener) was called for the entity "
                + listener.getEntityId());
        IActiveEntityEventsListener registeredListener = entityEventListeners.remove(listener.getEntityId());
        if (registeredListener == null) {
            throw new HyracksDataException(
                    "Active Entity Listener " + listener.getEntityId() + " hasn't been registered");
        }
    }

    public synchronized void monitorJob(JobId jobId, EntityId activeJob) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "monitorJob(JobId jobId, ActiveJob activeJob) called with job id: " + jobId);
            boolean found = jobId2ActiveJobInfos.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        if (entityEventListeners.containsKey(activeJob)) {
            if (jobId2ActiveJobInfos.containsKey(jobId)) {
                LOGGER.severe("Job is already being monitored for job: " + jobId);
                return;
            }
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "monitoring started for job id: " + jobId);
            }
        } else {
            LOGGER.severe("No listener was found for the entity: " + activeJob);
        }
        jobId2ActiveJobInfos.put(jobId, activeJob);
    }
}
