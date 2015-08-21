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

import java.util.Collection;
import java.util.List;

import org.json.JSONException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedActivity;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.message.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.message.FeedReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ScaleInReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;

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
