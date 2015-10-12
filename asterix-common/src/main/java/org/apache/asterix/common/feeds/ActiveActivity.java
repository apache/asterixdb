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

import java.util.Map;

public class ActiveActivity implements Comparable<ActiveActivity> {

    protected int activityId;

    protected final String dataverseName;
    protected final String objectName;
    protected final Map<String, String> activityDetails;

    public ActiveActivity(String dataverseName, String objectName, Map<String, String> activityDetails) {
        this.dataverseName = dataverseName;
        this.objectName = objectName;
        this.activityDetails = activityDetails;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ActiveActivity)) {
            return false;
        }

        if (!((ActiveActivity) other).dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!((ActiveActivity) other).objectName.equals(objectName)) {
            return false;
        }
        if (((ActiveActivity) other).getActivityId() != (activityId)) {
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
        return dataverseName + "." + objectName + " " + activityId;
    }

    public int getActivityId() {
        return activityId;
    }

    public void setActivityId(int activityId) {
        this.activityId = activityId;
    }

    public Map<String, String> getActivityDetails() {
        return activityDetails;
    }

    @Override
    public int compareTo(ActiveActivity o) {
        return o.getActivityId() - this.activityId;
    }

}