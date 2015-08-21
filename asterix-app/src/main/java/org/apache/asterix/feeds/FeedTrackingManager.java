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

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedTupleCommitAckMessage;
import edu.uci.ics.asterix.common.feeds.FeedTupleCommitResponseMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedTrackingManager;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedTrackingManager implements IFeedTrackingManager {

    private static final Logger LOGGER = Logger.getLogger(FeedTrackingManager.class.getName());

    private final BitSet allOnes;

    private Map<FeedConnectionId, Map<AckId, BitSet>> ackHistory;
    private Map<FeedConnectionId, Map<AckId, Integer>> maxBaseAcked;

    public FeedTrackingManager() {
        byte[] allOneBytes = new byte[128];
        Arrays.fill(allOneBytes, (byte) 0xff);
        allOnes = BitSet.valueOf(allOneBytes);
        ackHistory = new HashMap<FeedConnectionId, Map<AckId, BitSet>>();
        maxBaseAcked = new HashMap<FeedConnectionId, Map<AckId, Integer>>();
    }

    @Override
    public synchronized void submitAckReport(FeedTupleCommitAckMessage ackMessage) {
        AckId ackId = getAckId(ackMessage);
        Map<AckId, BitSet> acksForConnection = ackHistory.get(ackMessage.getConnectionId());
        if (acksForConnection == null) {
            acksForConnection = new HashMap<AckId, BitSet>();
            acksForConnection.put(ackId, BitSet.valueOf(ackMessage.getCommitAcks()));
            ackHistory.put(ackMessage.getConnectionId(), acksForConnection);
        }
        BitSet currentAcks = acksForConnection.get(ackId);
        if (currentAcks == null) {
            currentAcks = BitSet.valueOf(ackMessage.getCommitAcks());
            acksForConnection.put(ackId, currentAcks);
        } else {
            currentAcks.or(BitSet.valueOf(ackMessage.getCommitAcks()));
        }
        if (Arrays.equals(currentAcks.toByteArray(), allOnes.toByteArray())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(ackMessage.getIntakePartition() + " (" + ackMessage.getBase() + ")" + " is convered");
            }
            Map<AckId, Integer> maxBaseAckedForConnection = maxBaseAcked.get(ackMessage.getConnectionId());
            if (maxBaseAckedForConnection == null) {
                maxBaseAckedForConnection = new HashMap<AckId, Integer>();
                maxBaseAcked.put(ackMessage.getConnectionId(), maxBaseAckedForConnection);
            }
            Integer maxBaseAckedValue = maxBaseAckedForConnection.get(ackId);
            if (maxBaseAckedValue == null) {
                maxBaseAckedValue = ackMessage.getBase();
                maxBaseAckedForConnection.put(ackId, ackMessage.getBase());
                sendCommitResponseMessage(ackMessage.getConnectionId(), ackMessage.getIntakePartition(),
                        ackMessage.getBase());
            } else if (ackMessage.getBase() == maxBaseAckedValue + 1) {
                maxBaseAckedForConnection.put(ackId, ackMessage.getBase());
                sendCommitResponseMessage(ackMessage.getConnectionId(), ackMessage.getIntakePartition(),
                        ackMessage.getBase());
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Ignoring discountiuous acked base " + ackMessage.getBase() + " for " + ackId);
                }
            }

        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("AckId " + ackId + " pending number of acks " + (128 * 8 - currentAcks.cardinality()));
            }
        }
    }

    public synchronized void disableTracking(FeedConnectionId connectionId) {
        ackHistory.remove(connectionId);
        maxBaseAcked.remove(connectionId);
    }

    private void sendCommitResponseMessage(FeedConnectionId connectionId, int partition, int base) {
        FeedTupleCommitResponseMessage response = new FeedTupleCommitResponseMessage(connectionId, partition, base);
        List<String> storageLocations = FeedLifecycleListener.INSTANCE.getStoreLocations(connectionId);
        List<String> collectLocations = FeedLifecycleListener.INSTANCE.getCollectLocations(connectionId);
        String collectLocation = collectLocations.get(partition);
        Set<String> messageDestinations = new HashSet<String>();
        messageDestinations.add(collectLocation);
        messageDestinations.addAll(storageLocations);
        try {
            JobSpecification spec = FeedOperations.buildCommitAckResponseJob(response, messageDestinations);
            CentralFeedManager.runJob(spec, false);
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to send commit response message " + response + " exception " + e.getMessage());
            }
        }
    }

    private static AckId getAckId(FeedTupleCommitAckMessage ackMessage) {
        return new AckId(ackMessage.getConnectionId(), ackMessage.getIntakePartition(), ackMessage.getBase());
    }

    private static class AckId {
        private FeedConnectionId connectionId;
        private int intakePartition;
        private int base;

        public AckId(FeedConnectionId connectionId, int intakePartition, int base) {
            this.connectionId = connectionId;
            this.intakePartition = intakePartition;
            this.base = base;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AckId)) {
                return false;
            }
            AckId other = (AckId) o;
            return other.getConnectionId().equals(connectionId) && other.getIntakePartition() == intakePartition
                    && other.getBase() == base;
        }

        @Override
        public String toString() {
            return connectionId + "[" + intakePartition + "]" + "(" + base + ")";
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        public FeedConnectionId getConnectionId() {
            return connectionId;
        }

        public int getIntakePartition() {
            return intakePartition;
        }

        public int getBase() {
            return base;
        }

    }

    @Override
    public void disableAcking(FeedConnectionId connectionId) {
        ackHistory.remove(connectionId);
        maxBaseAcked.remove(connectionId);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Acking disabled for " + connectionId);
        }
    }

}