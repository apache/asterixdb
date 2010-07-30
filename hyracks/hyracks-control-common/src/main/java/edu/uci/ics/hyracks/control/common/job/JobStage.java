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
package edu.uci.ics.hyracks.control.common.job;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;

public class JobStage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID id;

    private final Set<ActivityNodeId> tasks;

    private final Set<JobStage> dependencies;

    private final Set<JobStage> dependents;

    private boolean started;

    public JobStage(Set<ActivityNodeId> tasks) {
        this.id = UUID.randomUUID();
        this.tasks = tasks;
        dependencies = new HashSet<JobStage>();
        dependents = new HashSet<JobStage>();
    }

    public UUID getId() {
        return id;
    }

    public Set<ActivityNodeId> getTasks() {
        return tasks;
    }

    public void addDependency(JobStage stage) {
        dependencies.add(stage);
    }

    public void addDependent(JobStage stage) {
        dependents.add(stage);
    }

    public Set<JobStage> getDependencies() {
        return dependencies;
    }

    @Override
    public int hashCode() {
        return id == null ? 0 : id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JobStage)) {
            return false;
        }
        return id == ((JobStage) o).id;
    }

    @Override
    public String toString() {
        return "SID:" + id + ": " + tasks;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted() {
        started = true;
    }
}