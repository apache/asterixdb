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
package edu.uci.ics.asterix.feeds;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedActivity;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedJobInfo.FeedJobState;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedTrackingManager;
import edu.uci.ics.asterix.common.feeds.message.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.message.FeedReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ScaleInReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.PrepareStallMessage;
import edu.uci.ics.asterix.metadata.feeds.TerminateDataFlowMessage;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedLoadManager implements IFeedLoadManager {

    private static final Logger LOGGER = Logger.getLogger(FeedLoadManager.class.getName());

    private static final long MIN_MODIFICATION_INTERVAL = 180000; // 10 seconds
    private final TreeSet<NodeLoadReport> nodeReports;
    private final Map<FeedConnectionId, FeedActivity> feedActivities;
    private final Map<String, Pair<Integer, Integer>> feedMetrics;

    private FeedConnectionId lastModified;
    private long lastModifiedTimestamp;

    private static final int UNKNOWN = -1;

    public FeedLoadManager() {
        this.nodeReports = new TreeSet<NodeLoadReport>();
        this.feedActivities = new HashMap<FeedConnectionId, FeedActivity>();
        this.feedMetrics = new HashMap<String, Pair<Integer, Integer>>();
    }

    @Override
    public void submitNodeLoadReport(NodeLoadReport report) {
        nodeReports.remove(report);
        nodeReports.add(report);
    }

    @Override
    public void reportCongestion(FeedCongestionMessage message) throws AsterixException {
        FeedRuntimeId runtimeId = message.getRuntimeId();
        FeedJobState jobState = FeedLifecycleListener.INSTANCE.getFeedJobState(message.getConnectionId());
        if (jobState == null
                || (jobState.equals(FeedJobState.UNDER_RECOVERY))
                || (message.getConnectionId().equals(lastModified) && System.currentTimeMillis()
                        - lastModifiedTimestamp < MIN_MODIFICATION_INTERVAL)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring congestion report from " + runtimeId);
            }
            return;
        } else {
            try {
                FeedLifecycleListener.INSTANCE.setJobState(message.getConnectionId(), FeedJobState.UNDER_RECOVERY);
                int inflowRate = message.getInflowRate();
                int outflowRate = message.getOutflowRate();
                List<String> currentComputeLocations = new ArrayList<String>();
                currentComputeLocations.addAll(FeedLifecycleListener.INSTANCE.getComputeLocations(message
                        .getConnectionId().getFeedId()));
                int computeCardinality = currentComputeLocations.size();
                int requiredCardinality = (int) Math
                        .ceil((double) ((computeCardinality * inflowRate) / (double) outflowRate)) + 5;
                int additionalComputeNodes = requiredCardinality - computeCardinality;
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("INCREASING COMPUTE CARDINALITY from " + computeCardinality + " by "
                            + additionalComputeNodes);
                }

                List<String> helperComputeNodes = getNodeForSubstitution(additionalComputeNodes);

                // Step 1) Alter the original feed job to adjust the cardinality
                JobSpecification jobSpec = FeedLifecycleListener.INSTANCE.getCollectJobSpecification(message
                        .getConnectionId());
                helperComputeNodes.addAll(currentComputeLocations);
                List<String> newLocations = new ArrayList<String>();
                newLocations.addAll(currentComputeLocations);
                newLocations.addAll(helperComputeNodes);
                FeedUtil.increaseCardinality(jobSpec, FeedRuntimeType.COMPUTE, requiredCardinality, newLocations);

                // Step 2) send prepare to  stall message
                gracefullyTerminateDataFlow(message.getConnectionId(), Integer.MAX_VALUE);

                // Step 3) run the altered job specification 
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("New Job after adjusting to the workload " + jobSpec);
                }

                Thread.sleep(10000);
                runJob(jobSpec, false);
                lastModified = message.getConnectionId();
                lastModifiedTimestamp = System.currentTimeMillis();

            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to form the required job for scaling in/out" + e.getMessage());
                }
                throw new AsterixException(e);
            }
        }
    }

    @Override
    public void submitScaleInPossibleReport(ScaleInReportMessage message) throws Exception {
        FeedJobState jobState = FeedLifecycleListener.INSTANCE.getFeedJobState(message.getConnectionId());
        if (jobState == null || (jobState.equals(FeedJobState.UNDER_RECOVERY))) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("JobState information for job " + "[" + message.getConnectionId() + "]" + " not found ");
            }
            return;
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Processing scale-in message " + message);
            }
            FeedLifecycleListener.INSTANCE.setJobState(message.getConnectionId(), FeedJobState.UNDER_RECOVERY);
            JobSpecification jobSpec = FeedLifecycleListener.INSTANCE.getCollectJobSpecification(message
                    .getConnectionId());
            int reducedCardinality = message.getReducedCardinaliy();
            List<String> currentComputeLocations = new ArrayList<String>();
            currentComputeLocations.addAll(FeedLifecycleListener.INSTANCE.getComputeLocations(message.getConnectionId()
                    .getFeedId()));
            FeedUtil.decreaseComputeCardinality(jobSpec, FeedRuntimeType.COMPUTE, reducedCardinality,
                    currentComputeLocations);

            gracefullyTerminateDataFlow(message.getConnectionId(), reducedCardinality - 1);
            Thread.sleep(3000);
            JobId newJobId = runJob(jobSpec, false);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Launch modified job" + "[" + newJobId + "]" + "for scale-in \n" + jobSpec);
            }

        }
    }

    private void gracefullyTerminateDataFlow(FeedConnectionId connectionId, int computePartitionRetainLimit)
            throws Exception {
        // Step 1) send prepare to  stall message
        PrepareStallMessage stallMessage = new PrepareStallMessage(connectionId, computePartitionRetainLimit);
        List<String> intakeLocations = FeedLifecycleListener.INSTANCE.getCollectLocations(connectionId);
        List<String> computeLocations = FeedLifecycleListener.INSTANCE.getComputeLocations(connectionId.getFeedId());
        List<String> storageLocations = FeedLifecycleListener.INSTANCE.getStoreLocations(connectionId);

        Set<String> operatorLocations = new HashSet<String>();

        operatorLocations.addAll(intakeLocations);
        operatorLocations.addAll(computeLocations);
        operatorLocations.addAll(storageLocations);

        JobSpecification messageJobSpec = FeedOperations.buildPrepareStallMessageJob(stallMessage, operatorLocations);
        runJob(messageJobSpec, true);

        // Step 2)
        TerminateDataFlowMessage terminateMesg = new TerminateDataFlowMessage(connectionId);
        messageJobSpec = FeedOperations.buildTerminateFlowMessageJob(terminateMesg, intakeLocations);
        runJob(messageJobSpec, true);
    }

    public static JobId runJob(JobSpecification spec, boolean waitForCompletion) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobId jobId = hcc.startJob(spec);
        if (waitForCompletion) {
            hcc.waitForCompletion(jobId);
        }
        return jobId;
    }

    @Override
    public void submitFeedRuntimeReport(FeedReportMessage report) {
        String key = "" + report.getConnectionId() + ":" + report.getRuntimeId().getFeedRuntimeType();
        Pair<Integer, Integer> value = feedMetrics.get(key);
        if (value == null) {
            value = new Pair<Integer, Integer>(report.getValue(), 1);
            feedMetrics.put(key, value);
        } else {
            value.first = value.first + report.getValue();
            value.second = value.second + 1;
        }
    }

    @Override
    public int getOutflowRate(FeedConnectionId connectionId, FeedRuntimeType runtimeType) {
        int rVal;
        String key = "" + connectionId + ":" + runtimeType;
        feedMetrics.get(key);
        Pair<Integer, Integer> value = feedMetrics.get(key);
        if (value == null) {
            rVal = UNKNOWN;
        } else {
            rVal = value.first / value.second;
        }
        return rVal;
    }

    private List<String> getNodeForSubstitution(int nRequired) {
        List<String> nodeIds = new ArrayList<String>();
        Iterator<NodeLoadReport> it = null;
        int nAdded = 0;
        while (nAdded < nRequired) {
            it = nodeReports.iterator();
            while (it.hasNext()) {
                nodeIds.add(it.next().getNodeId());
                nAdded++;
            }
        }
        return nodeIds;
    }

    @Override
    public synchronized List<String> getNodes(int required) {
        Iterator<NodeLoadReport> it;
        List<String> allocated = new ArrayList<String>();
        while (allocated.size() < required) {
            it = nodeReports.iterator();
            while (it.hasNext() && allocated.size() < required) {
                allocated.add(it.next().getNodeId());
            }
        }
        return allocated;
    }

    @Override
    public void reportThrottlingEnabled(ThrottlingEnabledFeedMessage mesg) throws AsterixException, Exception {
        System.out.println("Throttling Enabled for " + mesg.getConnectionId() + " " + mesg.getFeedRuntimeId());
        FeedConnectionId connectionId = mesg.getConnectionId();
        List<String> destinationLocations = new ArrayList<String>();
        List<String> storageLocations = FeedLifecycleListener.INSTANCE.getStoreLocations(connectionId);
        List<String> collectLocations = FeedLifecycleListener.INSTANCE.getCollectLocations(connectionId);

        destinationLocations.addAll(storageLocations);
        destinationLocations.addAll(collectLocations);
        JobSpecification messageJobSpec = FeedOperations.buildNotifyThrottlingEnabledMessageJob(mesg,
                destinationLocations);
        runJob(messageJobSpec, true);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.warning("Acking disabled for " + mesg.getConnectionId() + " in view of activated throttling");
        }
        IFeedTrackingManager trackingManager = CentralFeedManager.getInstance().getFeedTrackingManager();
        trackingManager.disableAcking(connectionId);
    }

    @Override
    public void reportFeedActivity(FeedConnectionId connectionId, FeedActivity activity) {
        feedActivities.put(connectionId, activity);
    }

    @Override
    public FeedActivity getFeedActivity(FeedConnectionId connectionId) {
        return feedActivities.get(connectionId);
    }

    @Override
    public Collection<FeedActivity> getFeedActivities() {
        return feedActivities.values();
    }

    @Override
    public void removeFeedActivity(FeedConnectionId connectionId) {
        feedActivities.remove(connectionId);
    }
}
