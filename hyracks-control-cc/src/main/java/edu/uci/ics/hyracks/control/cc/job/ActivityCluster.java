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
package edu.uci.ics.hyracks.control.cc.job;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;

public class ActivityCluster {
    private final JobRun jobRun;

    private final Set<ActivityId> activities;

    private final Set<ActivityCluster> dependencies;

    private final Set<ActivityCluster> dependents;

    private ActivityClusterPlan acp;

    public ActivityCluster(JobRun jobRun, Set<ActivityId> activities) {
        this.jobRun = jobRun;
        this.activities = activities;
        dependencies = new HashSet<ActivityCluster>();
        dependents = new HashSet<ActivityCluster>();
    }

    public Set<ActivityId> getActivities() {
        return activities;
    }

    public void addDependency(ActivityCluster stage) {
        dependencies.add(stage);
    }

    public void addDependent(ActivityCluster stage) {
        dependents.add(stage);
    }

    public Set<ActivityCluster> getDependencies() {
        return dependencies;
    }

    public Set<ActivityCluster> getDependents() {
        return dependents;
    }

    public JobRun getJobRun() {
        return jobRun;
    }

    public int getMaxTaskClusterAttempts() {
        return jobRun.getJobActivityGraph().getJobSpecification().getMaxAttempts();
    }

    public ActivityClusterPlan getPlan() {
        return acp;
    }

    public void setPlan(ActivityClusterPlan acp) {
        this.acp = acp;
    }
}