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

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.control.cc.scheduler.ISchedule;

public class JobStageAttempt {
    private final JobStage stage;

    private final JobAttempt jobAttempt;

    private final Set<String> participatingNodes;

    private final Set<String> completedNodes;

    private ISchedule schedule;

    public JobStageAttempt(JobStage stage, JobAttempt jobAttempt) {
        this.stage = stage;
        this.jobAttempt = jobAttempt;
        participatingNodes = new HashSet<String>();
        completedNodes = new HashSet<String>();
    }

    public JobStage getJobStage() {
        return stage;
    }

    public JobAttempt getJobAttempt() {
        return jobAttempt;
    }

    public void setSchedule(ISchedule schedule) {
        this.schedule = schedule;
        for (ActivityNodeId aid : stage.getTasks()) {
            String[] partitions = schedule.getPartitions(aid);
            for (String nid : partitions) {
                participatingNodes.add(nid);
            }
        }
    }

    public ISchedule getSchedule() {
        return schedule;
    }

    public Set<String> getParticipatingNodes() {
        return participatingNodes;
    }

    public Set<String> getCompletedNodes() {
        return completedNodes;
    }
}