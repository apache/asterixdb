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

import org.apache.asterix.active.ActiveEvent.EventKind;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveJobNotificationHandler implements Runnable {
    public static final ActiveJobNotificationHandler INSTANCE = new ActiveJobNotificationHandler();
    public static final String ACTIVE_ENTITY_PROPERTY_NAME = "ActiveJob";
    private static final Logger LOGGER = Logger.getLogger(ActiveJobNotificationHandler.class.getName());
    private static final boolean DEBUG = false;
    private final LinkedBlockingQueue<ActiveEvent> eventInbox;
    private final Map<EntityId, IActiveEntityEventsListener> entityEventListener;
    private final Map<JobId, ActiveJob> jobId2ActiveJobInfos;

    private ActiveJobNotificationHandler() {
        this.eventInbox = new LinkedBlockingQueue<>();
        this.jobId2ActiveJobInfos = new HashMap<>();
        this.entityEventListener = new HashMap<>();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(ActiveJobNotificationHandler.class.getSimpleName());
        LOGGER.log(Level.INFO, "Started " + ActiveJobNotificationHandler.class.getSimpleName());
        while (!Thread.interrupted()) {
            try {
                ActiveEvent event = getEventInbox().take();
                ActiveJob jobInfo = jobId2ActiveJobInfos.get(event.getJobId());
                EntityId entityId = jobInfo.getEntityId();
                IActiveEntityEventsListener listener = entityEventListener.get(entityId);
                if (DEBUG) {
                    LOGGER.log(Level.WARNING, "Next event is of type " + event.getEventKind());
                    LOGGER.log(Level.WARNING, "Notifying the listener");
                }
                listener.notify(event);
                if (event.getEventKind() == EventKind.JOB_FINISH) {
                    removeFinishedJob(event.getJobId());
                    removeInactiveListener(listener);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error handling an active job event", e);
            }
        }
        LOGGER.log(Level.INFO, "Stopped " + ActiveJobNotificationHandler.class.getSimpleName());
    }

    private void removeFinishedJob(JobId jobId) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "Removing the job");
        }
        jobId2ActiveJobInfos.remove(jobId);
    }

    private void removeInactiveListener(IActiveEntityEventsListener listener) {
        if (!listener.isEntityActive()) {
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Removing the listener since it is not active anymore");
            }
            entityEventListener.remove(listener.getEntityId());
        }
    }

    public IActiveEntityEventsListener getActiveEntityListener(EntityId entityId) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getActiveEntityListener(EntityId entityId) was called with entity " + entityId);
            IActiveEntityEventsListener listener = entityEventListener.get(entityId);
            LOGGER.log(Level.WARNING, "Listener found: " + listener);
        }
        return entityEventListener.get(entityId);
    }

    public synchronized ActiveJob[] getActiveJobs() {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getActiveJobs()  was called");
            LOGGER.log(Level.WARNING, "Number of jobs found: " + jobId2ActiveJobInfos.size());
        }
        return jobId2ActiveJobInfos.values().toArray(new ActiveJob[jobId2ActiveJobInfos.size()]);
    }

    public boolean isActiveJob(JobId jobId) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "isActiveJob(JobId jobId) called with jobId: " + jobId);
            boolean found = jobId2ActiveJobInfos.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        return jobId2ActiveJobInfos.get(jobId) != null;
    }

    public EntityId getEntity(JobId jobId) {
        ActiveJob jobInfo = jobId2ActiveJobInfos.get(jobId);
        return jobInfo == null ? null : jobInfo.getEntityId();
    }

    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING,
                    "notifyJobCreation(JobId jobId, JobSpecification jobSpecification) was called with jobId = "
                            + jobId);
        }
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (property == null || !(property instanceof ActiveJob)) {
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Job was is not active. property found to be: " + property);
            }
            return;
        } else {
            monitorJob(jobId, (ActiveJob) property);
        }
        if (DEBUG) {
            boolean found = jobId2ActiveJobInfos.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        ActiveJob jobInfo = jobId2ActiveJobInfos.get(jobId);
        if (jobInfo != null) {
            EntityId entityId = jobInfo.getEntityId();
            IActiveEntityEventsListener listener = entityEventListener.get(entityId);
            listener.notifyJobCreation(jobId, jobSpecification);
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Listener was notified" + jobId);
            }
        } else {
            if (DEBUG) {
                LOGGER.log(Level.WARNING,
                        "Listener was not notified since it was not registered for the job " + jobId);
            }
        }
    }

    public LinkedBlockingQueue<ActiveEvent> getEventInbox() {
        return eventInbox;
    }

    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getEventListeners() was called");
            LOGGER.log(Level.WARNING, "returning " + entityEventListener.size() + " Listeners");
        }
        return entityEventListener.values().toArray(new IActiveEntityEventsListener[entityEventListener.size()]);
    }

    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (DEBUG) {
            LOGGER.log(Level.WARNING,
                    "registerListener(IActiveEntityEventsListener listener) was called for the entity "
                            + listener.getEntityId());
        }
        if (entityEventListener.containsKey(listener.getEntityId())) {
            throw new HyracksDataException(
                    "Active Entity Listener " + listener.getEntityId() + " is already registered");
        }
        entityEventListener.put(listener.getEntityId(), listener);
    }

    public synchronized void monitorJob(JobId jobId, ActiveJob activeJob) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "monitorJob(JobId jobId, ActiveJob activeJob) called with job id: " + jobId);
            boolean found = jobId2ActiveJobInfos.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        if (entityEventListener.containsKey(activeJob.getEntityId())) {
            if (jobId2ActiveJobInfos.containsKey(jobId)) {
                LOGGER.severe("Job is already being monitored for job: " + jobId);
                return;
            }
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "monitoring started for job id: " + jobId);
            }
            jobId2ActiveJobInfos.put(jobId, activeJob);
        } else {
            LOGGER.severe("No listener was found for the entity: " + activeJob.getEntityId());
        }
    }
}
