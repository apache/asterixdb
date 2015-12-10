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
package org.apache.asterix.common.active.api;

import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.apache.asterix.common.active.ActiveActivity;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.active.message.FeedCongestionMessage;
import org.apache.asterix.common.active.message.FeedReportMessage;
import org.apache.asterix.common.active.message.ScaleInReportMessage;
import org.apache.asterix.common.active.message.ThrottlingEnabledFeedMessage;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.NodeLoadReport;

public interface IActiveLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(FeedCongestionMessage message) throws JSONException, AsterixException;

    public void submitFeedRuntimeReport(FeedReportMessage message);

    public void submitScaleInPossibleReport(ScaleInReportMessage sm) throws AsterixException, Exception;

    public List<String> getNodes(int required);

    public void reportThrottlingEnabled(ThrottlingEnabledFeedMessage mesg) throws AsterixException, Exception;

    int getOutflowRate(ActiveJobId connectionId, ActiveRuntimeType runtimeType);

    void reportActivity(ActiveJobId activeJobId, ActiveActivity activity);

    void removeActivity(ActiveJobId activeJobId);

    public ActiveActivity getActivity(ActiveJobId connectionId);

    public Collection<ActiveActivity> getActivities();

}
