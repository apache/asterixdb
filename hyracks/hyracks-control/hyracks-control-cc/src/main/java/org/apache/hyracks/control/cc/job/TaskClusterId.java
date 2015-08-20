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
package edu.uci.ics.hyracks.control.cc.job;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.job.ActivityClusterId;

public final class TaskClusterId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ActivityClusterId activityClusterId;

    private final int id;

    public TaskClusterId(ActivityClusterId activityClusterId, int id) {
        this.activityClusterId = activityClusterId;
        this.id = id;
    }

    public ActivityClusterId getActivityClusterId() {
        return activityClusterId;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((activityClusterId == null) ? 0 : activityClusterId.hashCode());
        result = prime * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TaskClusterId other = (TaskClusterId) obj;
        if (activityClusterId == null) {
            if (other.activityClusterId != null)
                return false;
        } else if (!activityClusterId.equals(other.activityClusterId))
            return false;
        if (id != other.id)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TC:" + activityClusterId + ":" + id;
    }
}