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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;

public final class ActivityClusterId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final JobId jobId;

    private final int id;

    public ActivityClusterId(JobId jobId, int id) {
        this.jobId = jobId;
        this.id = id;
    }

    public JobId getJobId() {
        return jobId;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ActivityClusterId other = (ActivityClusterId) obj;
        if (id != other.id) {
            return false;
        }
        if (jobId == null) {
            if (other.jobId != null) {
                return false;
            }
        } else if (!jobId.equals(other.jobId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ACID:" + jobId + ":" + id;
    }
}