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
package org.apache.asterix.common.feeds;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedMetricCollector;

public class FeedMetricCollector implements IFeedMetricCollector {

    private static final Logger LOGGER = Logger.getLogger(FeedMetricCollector.class.getName());

    private static final int UNKNOWN = -1;

    private final String nodeId;
    private final AtomicInteger globalSenderId = new AtomicInteger(1);
    private final Map<Integer, Sender> senders = new HashMap<Integer, Sender>();
    private final Map<Integer, Series> statHistory = new HashMap<Integer, Series>();
    private final Map<String, Sender> sendersByName = new HashMap<String, Sender>();

    public FeedMetricCollector(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public synchronized int createReportSender(FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            ValueType valueType, MetricType metricType) {
        Sender sender = new Sender(globalSenderId.getAndIncrement(), connectionId, runtimeId, valueType, metricType);
        senders.put(sender.senderId, sender);
        sendersByName.put(sender.getDisplayName(), sender);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Sender id " + sender.getSenderId() + " created for " + sender);
        }
        return sender.senderId;
    }

    @Override
    public void removeReportSender(int senderId) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            statHistory.remove(senderId);
            senders.remove(senderId);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to remove sender Id");
            }
            throw new IllegalStateException("Unable to remove sender Id " + senderId + " senders " + senders);
        }
    }

    @Override
    public boolean sendReport(int senderId, int value) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            Series series = statHistory.get(sender.senderId);
            if (series == null) {
                switch (sender.mType) {
                    case AVG:
                        series = new SeriesAvg();
                        break;
                    case RATE:
                        series = new SeriesRate();
                        break;
                }
                statHistory.put(sender.senderId, series);
            }
            series.addValue(value);
            return true;
        }
        throw new IllegalStateException("Unable to send report sender Id " + senderId + " senders " + senders);
    }

    @Override
    public void resetReportSender(int senderId) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            Series series = statHistory.get(sender.senderId);
            if (series != null) {
                series.reset();
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Sender with id " + senderId + " not found. Unable to reset!");
            }
            throw new IllegalStateException("Unable to reset sender Id " + senderId + " senders " + senders);
        }
    }

    private static class Sender {

        private final int senderId;
        private final MetricType mType;
        private final String displayName;

        public Sender(int senderId, FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType,
                MetricType mType) {
            this.senderId = senderId;
            this.mType = mType;
            this.displayName = createDisplayName(connectionId, runtimeId, valueType);
        }

        @Override
        public String toString() {
            return displayName + "[" + senderId + "]" + "(" + mType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Sender)) {
                return false;
            }
            return ((Sender) o).senderId == senderId;
        }

        @Override
        public int hashCode() {
            return senderId;
        }

        public static String createDisplayName(FeedConnectionId connectionId, FeedRuntimeId runtimeId,
                ValueType valueType) {
            return connectionId + " (" + runtimeId.getFeedRuntimeType() + " )" + "[" + runtimeId.getPartition() + "]"
                    + "{" + valueType + "}";
        }

        public String getDisplayName() {
            return displayName;
        }

        public int getSenderId() {
            return senderId;
        }
    }

    @Override
    public int getMetric(int senderId) {
        Sender sender = senders.get(senderId);
        return getMetric(sender);
    }

    @Override
    public int getMetric(FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType) {
        String displayName = Sender.createDisplayName(connectionId, runtimeId, valueType);
        Sender sender = sendersByName.get(displayName);
        return getMetric(sender);
    }

    private int getMetric(Sender sender) {
        if (sender == null || statHistory.get(sender.getSenderId()) == null) {
            return UNKNOWN;
        }

        float result = -1;
        Series series = statHistory.get(sender.getSenderId());
        switch (sender.mType) {
            case AVG:
                result = ((SeriesAvg) series).getAvg();
                break;
            case RATE:
                result = ((SeriesRate) series).getRate();
                break;
        }
        return (int) result;
    }

}
