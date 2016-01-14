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
package org.apache.asterix.external.feed.api;

import java.util.Collection;
import java.util.List;

import org.json.JSONException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.message.FeedCongestionMessage;
import org.apache.asterix.external.feed.message.FeedReportMessage;
import org.apache.asterix.external.feed.message.ScaleInReportMessage;
import org.apache.asterix.external.feed.message.ThrottlingEnabledFeedMessage;
import org.apache.asterix.external.feed.watch.FeedActivity;
import org.apache.asterix.external.feed.watch.NodeLoadReport;

public interface IFeedLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(FeedCongestionMessage message) throws JSONException, AsterixException;

    public void submitFeedRuntimeReport(FeedReportMessage message);

    public void submitScaleInPossibleReport(ScaleInReportMessage sm) throws AsterixException, Exception;

    public List<String> getNodes(int required);

    public void reportThrottlingEnabled(ThrottlingEnabledFeedMessage mesg) throws AsterixException, Exception;

    int getOutflowRate(FeedConnectionId connectionId, FeedRuntimeType runtimeType);

    void reportFeedActivity(FeedConnectionId connectionId, FeedActivity activity);

    void removeFeedActivity(FeedConnectionId connectionId);
    
    public FeedActivity getFeedActivity(FeedConnectionId connectionId);

    public Collection<FeedActivity> getFeedActivities();

}
