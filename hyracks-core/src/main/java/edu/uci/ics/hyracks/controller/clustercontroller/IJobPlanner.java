package edu.uci.ics.hyracks.controller.clustercontroller;

import java.util.Set;

import edu.uci.ics.hyracks.api.job.JobStage;

public interface IJobPlanner {
    public Set<String> plan(JobControl jobControl, JobStage stage) throws Exception;
}