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
package org.apache.asterix.common.channels;

import java.util.List;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ProcedureJobInfo extends ActiveJobInfo {

    //The locations of operators for the job
    private List<String> location;

    public ProcedureJobInfo(JobId jobId, JobState state, ActiveJopType jobType, JobSpecification spec,
            ActiveJobId channelJobId) {
        super(jobId, state, jobType, spec, channelJobId);
    }

    public String toString() {
        return jobId + " [" + jobType + "]";
    }

    public List<String> getLocation() {
        return location;
    }

    public void setLocation(List<String> location) {
        this.location = location;
    }

}
