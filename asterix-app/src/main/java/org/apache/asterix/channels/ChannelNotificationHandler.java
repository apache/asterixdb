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
package org.apache.asterix.channels;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobInfo.JobState;
import org.apache.asterix.common.channels.ChannelId;
import org.apache.asterix.common.channels.ChannelJobInfo;
import org.apache.asterix.common.channels.ChannelJobInfo.ChannelJobType;
import org.apache.asterix.common.channels.api.IChannelLifecycleEventSubscriber;
import org.apache.asterix.common.channels.api.IChannelLifecycleEventSubscriber.ChannelLifecycleEvent;
import org.apache.asterix.feeds.ActiveJobLifecycleListener.Message;
import org.apache.asterix.metadata.channels.ChannelMetaOperatorDescriptor;
import org.apache.asterix.metadata.channels.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

public class ChannelNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ChannelNotificationHandler.class.getName());

    private final LinkedBlockingQueue<Message> inbox;
    private final Map<ChannelId, List<IChannelLifecycleEventSubscriber>> eventSubscribers;

    private final Map<JobId, ChannelJobInfo> jobInfos;
    private final Map<ChannelId, ChannelJobInfo> channelJobInfos;

    public ChannelNotificationHandler(LinkedBlockingQueue<Message> inbox) {
        this.inbox = inbox;
        this.jobInfos = new HashMap<JobId, ChannelJobInfo>();
        this.channelJobInfos = new HashMap<ChannelId, ChannelJobInfo>();
        this.eventSubscribers = new HashMap<ChannelId, List<IChannelLifecycleEventSubscriber>>();
    }

    @Override
    public void run() {
        Message mesg;
        while (true) {
            try {
                mesg = inbox.take();
                switch (mesg.messageKind) {
                    case JOB_START:
                        handleJobStartMessage(mesg);
                        break;
                    case JOB_FINISH:
                        handleJobFinishMessage(mesg);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public Collection<ChannelJobInfo> getChannelInfos() {
        return channelJobInfos.values();
    }

    public void registerChannelJob(ChannelId channelId, JobId jobId, JobSpecification jobSpec) {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Channel job already registered");
        }

        ChannelJobInfo cInfo = new ChannelJobInfo(jobId, JobState.CREATED, ChannelJobType.REPETITIVE, jobSpec,
                channelId);
        jobInfos.put(jobId, cInfo);
        channelJobInfos.put(channelId, cInfo);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registered channel job [" + jobId + "]" + " for channel " + channelId);
        }

    }

    public void deregisterChannelJob(JobId jobId) {
        if (jobInfos.get(jobId) == null) {
            throw new IllegalStateException(" Channel job not registered ");
        }

        ChannelJobInfo info = (ChannelJobInfo) jobInfos.get(jobId);
        jobInfos.remove(jobId);
        channelJobInfos.remove(info.getChannelId());

        if (!info.getState().equals(JobState.UNDER_RECOVERY)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered channel job [" + jobId + "]");
            }
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not removing channel as job is in " + JobState.UNDER_RECOVERY + " state.");
            }
        }

    }

    private void handleJobStartMessage(Message message) throws Exception {
        ChannelJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case REPETITIVE:
                handleChannelJobStartMessage((ChannelJobInfo) jobInfo);
                break;
            case CONTINUOUS:
                handleChannelJobStartMessage((ChannelJobInfo) jobInfo);
                break;
        }
    }

    private void handleJobFinishMessage(Message message) throws Exception {
        ChannelJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case REPETITIVE:
                handleChannelJobFinishMessage((ChannelJobInfo) jobInfo, message);
                break;
            case CONTINUOUS:
                handleChannelJobFinishMessage((ChannelJobInfo) jobInfo, message);
                break;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Channel Job finished for channel " + jobInfo.getJobId());
        }
    }

    private synchronized void handleChannelJobStartMessage(ChannelJobInfo channelJobInfo) throws Exception {
        setLocations(channelJobInfo);

        channelJobInfo.setState(JobState.ACTIVE);

        // notify event listeners 
        notifyChannelEventSubscribers(channelJobInfo, ChannelLifecycleEvent.CHANNEL_STARTED);
    }

    private void notifyChannelEventSubscribers(ChannelJobInfo jobInfo, ChannelLifecycleEvent event) {
        List<IChannelLifecycleEventSubscriber> subscribers = eventSubscribers.get(jobInfo.getChannelId());
        if (subscribers != null && !subscribers.isEmpty()) {
            for (IChannelLifecycleEventSubscriber subscriber : subscribers) {
                subscriber.handleChannelEvent(event);
            }
        }

    }

    public Set<ChannelId> getActiveChannels() {
        Set<ChannelId> activeConnections = new HashSet<ChannelId>();
        for (ChannelJobInfo cInfo : channelJobInfos.values()) {
            if (cInfo.getState().equals(JobState.ACTIVE)) {
                activeConnections.add(cInfo.getChannelId());
            }
        }
        return activeConnections;
    }

    public boolean isChannelActive(ChannelId channelId) {
        ChannelJobInfo cInfo = channelJobInfos.get(channelId);
        if (cInfo != null) {
            return cInfo.getState().equals(JobState.ACTIVE);
        }
        return false;
    }

    public void setJobState(ChannelId channelId, JobState jobState) {
        ChannelJobInfo connectJobInfo = channelJobInfos.get(channelId);
        connectJobInfo.setState(jobState);
    }

    public JobState getChannelJobState(ChannelId channelId) {
        return channelJobInfos.get(channelId).getState();
    }

    private void handleChannelJobFinishMessage(ChannelJobInfo channelInfo, Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);

        deregisterChannelJob(message.jobId);

        // notify event listeners 
        JobStatus status = info.getStatus();
        ChannelLifecycleEvent event;
        event = status.equals(JobStatus.FAILURE) ? ChannelLifecycleEvent.CHANNEL_FAILURE
                : ChannelLifecycleEvent.CHANNEL_ENDED;
        notifyChannelEventSubscribers(channelInfo, event);

    }

    private void registerChannelActivity(ChannelJobInfo cInfo) {
        // TODO: Might want to add channels to the FeedLoadManager
        /*
        Map<String, String> channelActivityDetails = new HashMap<String, String>();

        if (cInfo.getLocation() != null) {
            channelActivityDetails.put(ChannelActivity.ChannelActivityDetails.CHANNEL_LOCATIONS,
                    StringUtils.join(cInfo.getLocation().iterator(), ','));
        }

        channelActivityDetails.put(ChannelActivity.ChannelActivityDetails.CHANNEL_TIMESTAMP, (new Date()).toString());
        try {
            ChannelActivity channelActivity = new ChannelActivity(cInfo.getChannelId().getDataverse(), cInfo
                    .getChannelId().getChannelName(), channelActivityDetails);
            CentralFeedManager.getInstance().getFeedLoadManager().reportFeedActivity(cInfo.getChannelId(), channelActivity);

        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to register channel activity for " + cInfo + " " + e.getMessage());
            }

        }*/

    }

    public void deregisterChannelActivity(ChannelJobInfo cInfo) {
        // TODO: Might want to add channels to the FeedLoadManager
        /*
        try {
            CentralFeedManager.getInstance().getFeedLoadManager().removeFeedActivity(cInfo.getConnectionId());
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to deregister feed activity for " + cInfo + " " + e.getMessage());
            }
        }*/
    }

    public boolean isRegisteredChannelJob(JobId jobId) {
        return jobInfos.get(jobId) != null;
    }

    public List<String> getChannelLocations(ChannelId channelId) {
        return channelJobInfos.get(channelId).getLocation();
    }

    public JobId getChannelJobId(ChannelId channelId) {
        return channelJobInfos.get(channelId).getJobId();
    }

    public ChannelJobInfo getChannelJobInfo(ChannelId channelId) {
        return channelJobInfos.get(channelId);
    }

    public void registerChannelEventSubscriber(ChannelId channelId, IChannelLifecycleEventSubscriber subscriber) {
        List<IChannelLifecycleEventSubscriber> subscribers = eventSubscribers.get(channelId);
        if (subscribers == null) {
            subscribers = new ArrayList<IChannelLifecycleEventSubscriber>();
            eventSubscribers.put(channelId, subscribers);
        }
        subscribers.add(subscriber);
    }

    public void deregisterChannelEventSubscriber(ChannelId channelId, IChannelLifecycleEventSubscriber subscriber) {
        List<IChannelLifecycleEventSubscriber> subscribers = eventSubscribers.get(channelId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
        }
    }

    public JobSpecification getChannelJobSpecification(ChannelId channelId) {
        return channelJobInfos.get(channelId).getSpec();
    }

    private void setLocations(ChannelJobInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> OperatorIds = new ArrayList<OperatorDescriptorId>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp = null;
            if (opDesc instanceof ChannelMetaOperatorDescriptor) {
                actualOp = ((ChannelMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof RepetitiveChannelOperatorDescriptor) {
                OperatorIds.add(entry.getKey());
            }
        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            List<String> locations = new ArrayList<String>();
            for (OperatorDescriptorId opId : OperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(opId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    locations.add(operatorLocations.get(i));
                }
            }

            cInfo.setLocation(locations);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}