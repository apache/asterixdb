package edu.uci.ics.hyracks.control.cc.job;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.cc.scheduler.IJobAttemptSchedulerState;
import edu.uci.ics.hyracks.control.cc.scheduler.IScheduler;

public class JobAttempt {
    private final JobRun jobRun;

    private final JobPlan plan;

    private final int attempt;

    private final JobStage endStage;

    private final JobProfile profile;

    private final Map<UUID, JobStage> stageMap;

    private final Map<UUID, JobStageAttempt> stageAttemptMap;

    private final Set<UUID> pendingStages;

    private final Set<UUID> completedStages;

    private final Set<UUID> inProgressStages;

    private final Set<String> participatingNodeIds;

    private final IJobAttemptSchedulerState schedulerState;

    public JobAttempt(JobRun jobRun, JobPlan plan, int attempt, IScheduler scheduler) {
        this.jobRun = jobRun;
        this.plan = plan;
        this.attempt = attempt;
        this.endStage = new JobPlanner().createStageDAG(plan);
        stageMap = new HashMap<UUID, JobStage>();
        stageAttemptMap = new HashMap<UUID, JobStageAttempt>();
        completedStages = new HashSet<UUID>();
        inProgressStages = new HashSet<UUID>();
        profile = new JobProfile(plan.getJobId(), attempt);
        populateJobStageMap(stageMap, endStage);
        pendingStages = new HashSet<UUID>(stageMap.keySet());
        participatingNodeIds = new HashSet<String>();
        schedulerState = scheduler.createJobAttemptState(this);
    }

    private static void populateJobStageMap(Map<UUID, JobStage> stageMap, JobStage stage) {
        stageMap.put(stage.getId(), stage);
        for (JobStage s : stage.getDependencies()) {
            populateJobStageMap(stageMap, s);
        }
    }

    public JobRun getJobRun() {
        return jobRun;
    }

    public JobPlan getPlan() {
        return plan;
    }

    public int getAttempt() {
        return attempt;
    }

    public JobStage getEndStage() {
        return endStage;
    }

    public void findRunnableStages(Set<JobStage> runnableStages) {
        findRunnableStages(runnableStages, endStage);
    }

    private void findRunnableStages(Set<JobStage> runnableStages, JobStage stage) {
        if (completedStages.contains(stage.getId()) || inProgressStages.contains(stage.getId())
                || runnableStages.contains(stage)) {
            return;
        }
        boolean runnable = true;
        for (JobStage s : stage.getDependencies()) {
            if (!completedStages.contains(s.getId())) {
                runnable = false;
                findRunnableStages(runnableStages, s);
            }
        }
        if (runnable) {
            runnableStages.add(stage);
        }
    }

    public Set<UUID> getPendingStageIds() {
        return pendingStages;
    }

    public Set<UUID> getInProgressStageIds() {
        return inProgressStages;
    }

    public Set<UUID> getCompletedStageIds() {
        return completedStages;
    }

    public Map<UUID, JobStage> getStageMap() {
        return stageMap;
    }

    public Map<UUID, JobStageAttempt> getStageAttemptMap() {
        return stageAttemptMap;
    }

    public Set<String> getParticipatingNodeIds() {
        return participatingNodeIds;
    }

    public JobProfile getJobProfile() {
        return profile;
    }

    public IJobAttemptSchedulerState getSchedulerState() {
        return schedulerState;
    }
}