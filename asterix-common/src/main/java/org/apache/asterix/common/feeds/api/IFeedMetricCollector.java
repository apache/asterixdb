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
package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;

public interface IFeedMetricCollector {

    public enum ValueType {
        CPU_USAGE,
        INFLOW_RATE,
        OUTFLOW_RATE
    }

    public enum MetricType {
        AVG,
        RATE
    }

    public boolean sendReport(int senderId, int value);

    public int getMetric(int senderId);

    public int getMetric(FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType);

    int createReportSender(FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType,
            MetricType metricType);

    public void removeReportSender(int senderId);

    public void resetReportSender(int senderId);

}
