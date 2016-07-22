/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.active;

import java.util.Map;

public class Activity implements Comparable<Activity> {

    protected int activityId;
    protected final EntityId activeEntityId;
    protected final Map<String, String> activityDetails;

    public Activity(EntityId activeEntityId, Map<String, String> activityDetails) {
        this.activeEntityId = activeEntityId;
        this.activityDetails = activityDetails;
    }

    public String getDataverseName() {
        return activeEntityId.getDataverse();
    }

    public String getActiveEntityName() {
        return activeEntityId.getEntityName();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Activity)) {
            return false;
        }
        return ((Activity) other).activeEntityId.equals(activeEntityId)
                && ((Activity) other).getActivityId() != (activityId);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return activeEntityId + "." + activityId;
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
    public int compareTo(Activity o) {
        return o.getActivityId() - this.activityId;
    }

}