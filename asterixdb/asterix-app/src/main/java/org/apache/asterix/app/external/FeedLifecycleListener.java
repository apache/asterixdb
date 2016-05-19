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
package org.apache.asterix.app.external;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IFeedLifecycleListener;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.management.FeedJointKey;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.feed.watch.FeedJobInfo.FeedJobState;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

/**
 * A listener that subscribes to events associated with cluster membership
 * (nodes joining/leaving the cluster) and job lifecycle (start/end of a job).
 * Subscription to such events allows keeping track of feed ingestion jobs and
 * take any corrective action that may be required when a node involved in a
 * feed leaves the cluster.
 */
public class FeedLifecycleListener implements IFeedLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(FeedLifecycleListener.class.getName());
    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    private final LinkedBlockingQueue<FeedEvent> jobEventInbox;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final ExecutorService executorService;

    private FeedLifecycleListener() {
        this.jobEventInbox = new LinkedBlockingQueue<FeedEvent>();
        this.feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(feedJobNotificationHandler);
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new FeedEvent(jobId, FeedEvent.EventKind.JOB_START));
        }
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new FeedEvent(jobId, FeedEvent.EventKind.JOB_FINISH));
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NO NEED TO NOTIFY JOB FINISH!");
            }
        }
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedConnectJobInfo(connectionId);
    }

    /*
     * Traverse job specification to categorize job as a feed intake job or a feed collection job
     */
    @Override
    public void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf) throws HyracksException {
        JobSpecification spec = acggf.getJobSpecification();
        FeedConnectionId feedConnectionId = null;
        Map<String, String> feedPolicy = null;
        for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                feedConnectionId = ((FeedCollectOperatorDescriptor) opDesc).getFeedConnectionId();
                feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicyProperties();
                feedJobNotificationHandler.registerFeedCollectionJob(
                        ((FeedCollectOperatorDescriptor) opDesc).getSourceFeedId(), feedConnectionId, jobId, spec,
                        feedPolicy);
                break;
            } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                feedJobNotificationHandler.registerFeedIntakeJob(((FeedIntakeOperatorDescriptor) opDesc).getFeedId(),
                        jobId, spec);
                break;
            }
        }
    }

    public void setJobState(FeedConnectionId connectionId, FeedJobState jobState) {
        feedJobNotificationHandler.setJobState(connectionId, jobState);
    }

    public FeedJobState getFeedJobState(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedJobState(connectionId);
    }

    public static class FeedEvent {
        public JobId jobId;
        public FeedId feedId;

        public enum EventKind {
            JOB_START,
            JOB_FINISH,
            PARTITION_START
        }

        public EventKind eventKind;

        public FeedEvent(JobId jobId, EventKind eventKind) {
            this(jobId, eventKind, null);
        }

        public FeedEvent(JobId jobId, EventKind eventKind, FeedId feedId) {
            this.jobId = jobId;
            this.eventKind = eventKind;
            this.feedId = feedId;
        }
    }

    public void submitFeedConnectionRequest(IFeedJoint feedPoint, FeedConnectionRequest subscriptionRequest)
            throws Exception {
        feedJobNotificationHandler.submitFeedConnectionRequest(feedPoint, subscriptionRequest);
    }

    @Override
    public List<FeedConnectionId> getActiveFeedConnections(FeedId feedId) {
        List<FeedConnectionId> connections = new ArrayList<FeedConnectionId>();
        Collection<FeedConnectionId> activeConnections = feedJobNotificationHandler.getActiveFeedConnections();
        if (feedId != null) {
            for (FeedConnectionId connectionId : activeConnections) {
                if (connectionId.getFeedId().equals(feedId)) {
                    connections.add(connectionId);
                }
            }
        } else {
            connections.addAll(activeConnections);
        }
        return connections;
    }

    @Override
    public List<String> getComputeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedComputeLocations(feedId);
    }

    @Override
    public List<String> getIntakeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedIntakeLocations(feedId);
    }

    @Override
    public List<String> getStoreLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedStorageLocations(feedConnectionId);
    }

    @Override
    public List<String> getCollectLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedCollectLocations(feedConnectionId);
    }

    @Override
    public synchronized boolean isFeedConnectionActive(FeedConnectionId connectionId,
            IFeedLifecycleEventSubscriber eventSubscriber) {
        return feedJobNotificationHandler.isFeedConnectionActive(connectionId, eventSubscriber);
    }

    @Override
    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    @Override
    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.isFeedPointAvailable(feedJointKey);
    }

    public void registerFeedJoint(IFeedJoint feedJoint, int numOfPrividers) {
        feedJobNotificationHandler.registerFeedJoint(feedJoint, numOfPrividers);
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getFeedJoint(feedJointKey);
    }

    @Override
    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.registerFeedEventSubscriber(connectionId, subscriber);
    }

    @Override
    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.deregisterFeedEventSubscriber(connectionId, subscriber);

    }

    public synchronized void notifyPartitionStart(FeedId feedId, JobId jobId) {
        jobEventInbox.add(new FeedEvent(jobId, FeedEvent.EventKind.PARTITION_START, feedId));
    }

}
