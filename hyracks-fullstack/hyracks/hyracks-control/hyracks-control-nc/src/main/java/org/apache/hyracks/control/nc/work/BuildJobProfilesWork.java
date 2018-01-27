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
package org.apache.hyracks.control.nc.work;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.JobletProfile;
import org.apache.hyracks.control.common.work.FutureValue;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;

public class BuildJobProfilesWork extends SynchronizableWork {
    private final NodeControllerService ncs;

    private final CcId ccId;
    private final FutureValue<List<JobProfile>> fv;

    public BuildJobProfilesWork(NodeControllerService ncs, CcId ccId, FutureValue<List<JobProfile>> fv) {
        this.ncs = ncs;
        this.ccId = ccId;
        this.fv = fv;
    }

    @Override
    protected void doRun() throws Exception {
        List<JobProfile> profiles = new ArrayList<>();
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        jobletMap.values().stream().filter(ji -> ji.getJobId().getCcId().equals(ccId))
                .forEach(ji -> profiles.add(new JobProfile(ji.getJobId())));
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
