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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionMatchMaker;
import edu.uci.ics.hyracks.control.cc.scheduler.ActivityPartitionDetails;
import edu.uci.ics.hyracks.control.cc.scheduler.JobScheduler;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;

public class JobRun implements IJobStatusConditionVariable {
    private final JobId jobId;

    private final JobActivityGraph jag;

    private final PartitionMatchMaker pmm;

    private final Set<String> participatingNodeIds;

    private final JobProfile profile;

    private Set<ActivityCluster> activityClusters;

    private final Map<ActivityId, ActivityCluster> activityClusterMap;

    private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap;

    private JobScheduler js;

    private JobStatus status;

    private Exception exception;

    public JobRun(JobId jobId, JobActivityGraph plan) {
        this.jobId = jobId;
        this.jag = plan;
        pmm = new PartitionMatchMaker();
        participatingNodeIds = new HashSet<String>();
        profile = new JobProfile(jobId);
        activityClusterMap = new HashMap<ActivityId, ActivityCluster>();
        connectorPolicyMap = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();
    }

    public JobId getJobId() {
        return jobId;
    }

    public JobActivityGraph getJobActivityGraph() {
        return jag;
    }

    public PartitionMatchMaker getPartitionMatchMaker() {
        return pmm;
    }

    public synchronized void setStatus(JobStatus status, Exception exception) {
        this.status = status;
        this.exception = exception;
        notifyAll();
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized Exception getException() {
        return exception;
    }

    @Override
    public synchronized void waitForCompletion() throws Exception {
        while (status != JobStatus.TERMINATED && status != JobStatus.FAILURE) {
            wait();
        }
        if (exception != null) {
            throw new HyracksException("Job Failed", exception);
        }
    }

    public Set<String> getParticipatingNodeIds() {
        return participatingNodeIds;
    }

    public JobProfile getJobProfile() {
        return profile;
    }

    public void setScheduler(JobScheduler js) {
        this.js = js;
    }

    public JobScheduler getScheduler() {
        return js;
    }

    public Map<ActivityId, ActivityCluster> getActivityClusterMap() {
        return activityClusterMap;
    }

    public Set<ActivityCluster> getActivityClusters() {
        return activityClusters;
    }

    public void setActivityClusters(Set<ActivityCluster> activityClusters) {
        this.activityClusters = activityClusters;
    }

    public Map<ConnectorDescriptorId, IConnectorPolicy> getConnectorPolicyMap() {
        return connectorPolicyMap;
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject result = new JSONObject();

        result.put("job-id", jobId.toString());

        JSONArray aClusters = new JSONArray();
        for (ActivityCluster ac : activityClusters) {
            JSONObject acJSON = new JSONObject();

            acJSON.put("activity-cluster-id", String.valueOf(ac.getActivityClusterId()));

            JSONArray activitiesJSON = new JSONArray();
            for (ActivityId aid : ac.getActivities()) {
                activitiesJSON.put(aid);
            }
            acJSON.put("activities", activitiesJSON);

            JSONArray dependentsJSON = new JSONArray();
            for (ActivityCluster dependent : ac.getDependents()) {
                dependentsJSON.put(String.valueOf(dependent.getActivityClusterId()));
            }
            acJSON.put("dependents", dependentsJSON);

            JSONArray dependenciesJSON = new JSONArray();
            for (ActivityCluster dependency : ac.getDependencies()) {
                dependenciesJSON.put(String.valueOf(dependency.getActivityClusterId()));
            }
            acJSON.put("dependencies", dependenciesJSON);

            ActivityClusterPlan acp = ac.getPlan();
            if (acp == null) {
                acJSON.put("plan", (Object) null);
            } else {
                JSONObject planJSON = new JSONObject();

                JSONArray acTasks = new JSONArray();
                for (Map.Entry<ActivityId, ActivityPlan> e : acp.getActivityPlanMap().entrySet()) {
                    ActivityPlan acPlan = e.getValue();
                    JSONObject entry = new JSONObject();
                    entry.put("activity-id", e.getKey().toString());

                    ActivityPartitionDetails apd = acPlan.getActivityPartitionDetails();
                    entry.put("partition-count", apd.getPartitionCount());

                    JSONArray inPartCountsJSON = new JSONArray();
                    int[] inPartCounts = apd.getInputPartitionCounts();
                    if (inPartCounts != null) {
                        for (int i : inPartCounts) {
                            inPartCountsJSON.put(i);
                        }
                    }
                    entry.put("input-partition-counts", inPartCountsJSON);

                    JSONArray outPartCountsJSON = new JSONArray();
                    int[] outPartCounts = apd.getOutputPartitionCounts();
                    if (outPartCounts != null) {
                        for (int o : outPartCounts) {
                            outPartCountsJSON.put(o);
                        }
                    }
                    entry.put("output-partition-counts", outPartCountsJSON);

                    JSONArray tasks = new JSONArray();
                    for (Task t : acPlan.getTasks()) {
                        JSONObject task = new JSONObject();

                        task.put("task-id", t.getTaskId().toString());

                        JSONArray dependentTasksJSON = new JSONArray();
                        for (TaskId dependent : t.getDependents()) {
                            dependentTasksJSON.put(dependent.toString());
                        }
                        task.put("dependents", dependentTasksJSON);

                        JSONArray dependencyTasksJSON = new JSONArray();
                        for (TaskId dependency : t.getDependencies()) {
                            dependencyTasksJSON.put(dependency.toString());
                        }
                        task.put("dependencies", dependencyTasksJSON);

                        tasks.put(task);
                    }
                    entry.put("tasks", tasks);

                    acTasks.put(entry);
                }
                planJSON.put("task-map", acTasks);

                JSONArray tClusters = new JSONArray();
                for (TaskCluster tc : acp.getTaskClusters()) {
                    JSONObject c = new JSONObject();
                    c.put("task-cluster-id", String.valueOf(tc.getTaskClusterId()));

                    JSONArray tasks = new JSONArray();
                    for (Task t : tc.getTasks()) {
                        tasks.put(t.getTaskId().toString());
                    }
                    c.put("tasks", tasks);

                    JSONArray prodParts = new JSONArray();
                    for (PartitionId p : tc.getProducedPartitions()) {
                        prodParts.put(p.toString());
                    }
                    c.put("produced-partitions", prodParts);

                    JSONArray reqdParts = new JSONArray();
                    for (PartitionId p : tc.getRequiredPartitions()) {
                        reqdParts.put(p.toString());
                    }
                    c.put("required-partitions", reqdParts);

                    JSONArray attempts = new JSONArray();
                    List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
                    if (tcAttempts != null) {
                        for (TaskClusterAttempt tca : tcAttempts) {
                            JSONObject attempt = new JSONObject();
                            attempt.put("attempt", tca.getAttempt());
                            attempt.put("status", tca.getStatus());

                            JSONArray taskAttempts = new JSONArray();
                            for (TaskAttempt ta : tca.getTaskAttempts()) {
                                JSONObject taskAttempt = new JSONObject();
                                taskAttempt.put("task-attempt-id", ta.getTaskAttemptId());
                                taskAttempt.put("status", ta.getStatus());
                                taskAttempt.put("node-id", ta.getNodeId());
                                Exception e = ta.getException();
                                if (e != null) {
                                    JSONObject ex = new JSONObject();
                                    ex.put("exception-class", e.getClass().getName());
                                    ex.put("exception-message", e.getMessage());
                                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                    PrintWriter pw = new PrintWriter(baos);
                                    e.printStackTrace(pw);
                                    pw.close();
                                    ex.put("exception-stacktrace", new String(baos.toByteArray()));

                                    taskAttempt.put("exception", ex);
                                }
                                taskAttempts.put(taskAttempt);
                            }
                            attempt.put("task-attempts", taskAttempts);

                            attempts.put(attempt);
                        }
                    }
                    c.put("attempts", attempts);

                    tClusters.put(c);
                }
                planJSON.put("task-clusters", tClusters);

                acJSON.put("plan", planJSON);
            }
            aClusters.put(acJSON);
        }
        result.put("activity-clusters", aClusters);

        return result;
    }
}