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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobAttempt;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;

public class ReportProfilesEvent implements Runnable {
    private final ClusterControllerService ccs;
    private final List<JobProfile> profiles;

    public ReportProfilesEvent(ClusterControllerService ccs, List<JobProfile> profiles) {
        this.ccs = ccs;
        this.profiles = profiles;
    }

    @Override
    public void run() {
        Map<UUID, JobRun> runMap = ccs.getRunMap();
        for (JobProfile profile : profiles) {
            JobRun run = runMap.get(profile.getJobId());
            if (run != null) {
                JobAttempt ja = run.getAttempts().get(profile.getAttempt());
                ja.getJobProfile().merge(profile);
            }
        }
    }
}