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
package edu.uci.ics.hyracks.control.nc.work;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.control.common.work.FutureValue;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class BuildJobProfilesWork extends SynchronizableWork {
    private final NodeControllerService ncs;

    private final FutureValue<List<JobProfile>> fv;

    public BuildJobProfilesWork(NodeControllerService ncs, FutureValue<List<JobProfile>> fv) {
        this.ncs = ncs;
        this.fv = fv;
    }

    @Override
    protected void doRun() throws Exception {
        List<JobProfile> profiles = new ArrayList<JobProfile>();
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        for (Joblet ji : jobletMap.values()) {
            profiles.add(new JobProfile(ji.getJobId()));
        }
        for (JobProfile jProfile : profiles) {
            Joblet ji;
            JobletProfile jobletProfile = new JobletProfile(ncs.getId());
            ji = jobletMap.get(jProfile.getJobId());
            if (ji != null) {
                ji.dumpProfile(jobletProfile);
                jProfile.getJobletProfiles().put(ncs.getId(), jobletProfile);
            }
        }
        fv.setValue(profiles);
    }
}