/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package org.apache.asterix.common.channels;

import java.util.Map;

public class ChannelActivity implements Comparable<ChannelActivity> {

    private int activityId;

    private final String dataverseName;
    private final String channelName;
    private final Map<String, String> channelActivityDetails;

    public static class ChannelActivityDetails {
        public static final String CHANNEL_LOCATIONS = "locations";
        public static final String CHANNEL_TIMESTAMP = "channel-timestamp";

    }

    public ChannelActivity(String dataverseName, String channelName, Map<String, String> channelActivityDetails) {
        this.dataverseName = dataverseName;
        this.channelName = channelName;
        this.channelActivityDetails = channelActivityDetails;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getChannelName() {
        return channelName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ChannelActivity)) {
            return false;
        }

        if (!((ChannelActivity) other).dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!((ChannelActivity) other).getChannelName().equals(channelName)) {
            return false;
        }
        if (((ChannelActivity) other).getActivityId() != (activityId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return dataverseName + "." + channelName + " " + activityId;
    }

    public String getTimestamp() {
        return channelActivityDetails.get(ChannelActivityDetails.CHANNEL_TIMESTAMP);
    }

    public int getActivityId() {
        return activityId;
    }

    public void setActivityId(int activityId) {
        this.activityId = activityId;
    }

    public Map<String, String> getChannelActivityDetails() {
        return channelActivityDetails;
    }

    @Override
    public int compareTo(ChannelActivity o) {
        return o.getActivityId() - this.activityId;
    }

}