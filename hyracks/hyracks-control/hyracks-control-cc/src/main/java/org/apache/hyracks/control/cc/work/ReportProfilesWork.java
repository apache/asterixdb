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
package org.apache.hyracks.control.cc.work;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.work.AbstractWork;

public class ReportProfilesWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final List<JobProfile> profiles;

    public ReportProfilesWork(ClusterControllerService ccs, List<JobProfile> profiles) {
        this.ccs = ccs;
        this.profiles = profiles;
    }

    @Override
    public void run() {
        Map<JobId, JobRun> runMap = ccs.getActiveRunMap();
        for (JobProfile profile : profiles) {
            JobRun run = runMap.get(profile.getJobId());
            if (run != null) {
                JobProfile jp = run.getJobProfile();
                jp.merge(profile);
            }
        }
    }

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }
}