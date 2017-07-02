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
package org.apache.asterix.external.feed.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.StatsRequestMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;

public class ActiveEntityEventsListener implements IActiveEntityEventsListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveEntityEventsListener.class.getName());

    enum RequestState {
        INIT,
        STARTED,
        FINISHED
    }

    // members
    protected volatile ActivityState state;
    protected JobId jobId;
    protected final List<IActiveEventSubscriber> subscribers = new ArrayList<>();
    protected final ICcApplicationContext appCtx;
    protected final EntityId entityId;
    protected final List<IDataset> datasets;
    protected final ActiveEvent statsUpdatedEvent;
    protected long statsTimestamp;
    protected String stats;
    protected RequestState statsRequestState;
    protected final String runtimeName;
    protected final AlgebricksAbsolutePartitionConstraint locations;
    protected int numRegistered;

    public ActiveEntityEventsListener(ICcApplicationContext appCtx, EntityId entityId, List<IDataset> datasets,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName) {
        this.appCtx = appCtx;
        this.entityId = entityId;
        this.datasets = datasets;
        this.state = ActivityState.STOPPED;
        this.statsTimestamp = -1;
        this.statsRequestState = RequestState.INIT;
        this.statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId);
        this.stats = "{\"Stats\":\"N/A\"}";
        this.runtimeName = runtimeName;
        this.locations = locations;
        this.numRegistered = 0;
    }

    @Override
    public synchronized void notify(ActiveEvent event) {
        try {
            LOGGER.finer("EventListener is notified.");
            ActiveEvent.Kind eventKind = event.getEventKind();
            switch (eventKind) {
                case JOB_CREATED:
                    state = ActivityState.CREATED;
                    break;
                case JOB_STARTED:
                    start(event);
                    break;
                case JOB_FINISHED:
                    finish();
                    break;
                case PARTITION_EVENT:
                    handle((ActivePartitionMessage) event.getEventObject());
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Unhandled feed event notification: " + event);
                    break;
            }
            notifySubscribers(event);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    protected synchronized void handle(ActivePartitionMessage message) {
        if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == locations.getLocations().length) {
                state = ActivityState.STARTED;
            }
        }
    }

    private void finish() throws Exception {
        IHyracksClientConnection hcc = appCtx.getHcc();
        JobStatus status = hcc.getJobStatus(jobId);
        state = status.equals(JobStatus.FAILURE) ? ActivityState.FAILED : ActivityState.STOPPED;
        ActiveLifecycleListener activeLcListener = (ActiveLifecycleListener) appCtx.getActiveLifecycleListener();
        activeLcListener.getNotificationHandler().removeListener(this);
    }

    private void start(ActiveEvent event) {
        this.jobId = event.getJobId();
        state = ActivityState.STARTING;
    }

    @Override
    public synchronized void subscribe(IActiveEventSubscriber subscriber) throws HyracksDataException {
        if (this.state == ActivityState.FAILED) {
            throw new RuntimeDataException(ErrorCode.CANNOT_SUBSCRIBE_TO_FAILED_ACTIVE_ENTITY);
        }
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
    public boolean isEntityUsingDataset(IDataset dataset) {
        return datasets.contains(dataset);
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
        LOGGER.log(Level.INFO, "refreshStats called");
        synchronized (this) {
            if (state != ActivityState.STARTED || statsRequestState == RequestState.STARTED) {
                LOGGER.log(Level.INFO, "returning immediately since state = " + state + " and statsRequestState = "
                        + statsRequestState);
                return;
            } else {
                statsRequestState = RequestState.STARTED;
            }
        }
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
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
        // Same as above
        statsRequestState = RequestState.FINISHED;
    }

    protected synchronized void notifySubscribers(ActiveEvent event) {
        notifyAll();
        Iterator<IActiveEventSubscriber> it = subscribers.iterator();
        while (it.hasNext()) {
            IActiveEventSubscriber subscriber = it.next();
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

}
