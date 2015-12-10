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
package org.apache.asterix.common.feeds.api;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveRuntimeId;

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

    public int getMetric(ActiveJobId connectionId, ActiveRuntimeId runtimeId, ValueType valueType);

    int createReportSender(ActiveJobId connectionId, ActiveRuntimeId runtimeId, ValueType valueType,
            MetricType metricType);

    public void removeReportSender(int senderId);

    public void resetReportSender(int senderId);

}
