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
package org.apache.hyracks.control.cc.adminconsole.pages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.util.string.StringValue;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.work.GetActivityClusterGraphJSONWork;
import org.apache.hyracks.control.cc.work.GetJobRunJSONWork;

public class JobDetailsPage extends AbstractPage {
    private static final long serialVersionUID = 1L;

    private static final int HEIGHT = 29;

    public JobDetailsPage(PageParameters params) throws Exception {
        ClusterControllerService ccs = getAdminConsoleApplication().getClusterControllerService();

        StringValue jobIdStr = params.get("job-id");

        JobId jobId = JobId.parse(jobIdStr.toString());

        GetActivityClusterGraphJSONWork gacgw = new GetActivityClusterGraphJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gacgw);
        Label jag = new Label("activity-cluster-graph", gacgw.getJSON().toString());
        jag.setEscapeModelStrings(false);
        add(jag);

        ObjectNode jagO = gacgw.getJSON();

        Map<ActivityId, String> activityMap = new HashMap<ActivityId, String>();
        if (jagO.has("activity-clusters")) {
            JsonNode acArray = jagO.get("activity-clusters");
            for (int j = 0; j < acArray.size(); ++j) {
                JsonNode acO = acArray.get(j);
                if (acO.has("activities")) {
                    JsonNode aArray = acO.get("activities");
                    for (int i = 0; i < aArray.size(); ++i) {
                        JsonNode aO = aArray.get(i);
                        ActivityId aid = ActivityId.parse(aO.get("id").asText());
                        String className = aO.get("java-class").asText();

                        activityMap.put(aid, className);
                    }
                }
            }
        }

        GetJobRunJSONWork gjrw = new GetJobRunJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjrw);
        Label jobrun = new Label("job-run", gjrw.getJSON().toString());
        jobrun.setEscapeModelStrings(false);
        add(jobrun);

        ObjectNode jrO = gjrw.getJSON();

        List<TaskClusterAttempt[]> tcList = new ArrayList<TaskClusterAttempt[]>();
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        if (jrO.has("activity-clusters")) {
            JsonNode acA = jrO.get("activity-clusters");
            for (int i = 0; i < acA.size(); ++i) {
                JsonNode acO = acA.get(i);
                if (acO.has("plan")) {
                    JsonNode planO = acO.get("plan");
                    if (planO.has("task-clusters")) {
                        JsonNode tcA = planO.get("task-clusters");
                        for (int j = 0; j < tcA.size(); ++j) {
                            JsonNode tcO = tcA.get(j);
                            String tcId = tcO.get("task-cluster-id").asText();
                            if (tcO.has("attempts")) {
                                JsonNode tcaA = tcO.get("attempts");
                                TaskClusterAttempt[] tcAttempts = new TaskClusterAttempt[tcaA.size()];
                                for (int k = 0; k < tcaA.size(); ++k) {
                                    JsonNode tcaO = tcaA.get(k);
                                    int attempt = tcaO.get("attempt").asInt();
                                    long startTime = tcaO.get("start-time").asLong();
                                    long endTime = tcaO.get("end-time").asLong();

                                    tcAttempts[k] = new TaskClusterAttempt(tcId, attempt, startTime, endTime);
                                    if (startTime < minTime) {
                                        minTime = startTime;
                                    }
                                    if (endTime > maxTime) {
                                        maxTime = endTime;
                                    }
                                    if (tcaO.has("task-attempts")) {
                                        JsonNode taArray = tcaO.get("task-attempts");
                                        tcAttempts[k].tasks = new TaskAttempt[taArray.size()];
                                        for (int l = 0; l < taArray.size(); ++l) {
                                            JsonNode taO = taArray.get(l);
                                            TaskAttemptId taId = TaskAttemptId.parse(taO.get("task-attempt-id").asText());
                                            TaskAttempt ta = new TaskAttempt(taId, taO.get("start-time").asLong(),
                                                    taO.get("end-time").asLong());
                                            tcAttempts[k].tasks[l] = ta;
                                            TaskId tid = taId.getTaskId();
                                            ta.name = activityMap.get(tid.getActivityId());
                                            ta.partition = tid.getPartition();
                                        }
                                        Arrays.sort(tcAttempts[k].tasks, new Comparator<TaskAttempt>() {
                                            @Override
                                            public int compare(TaskAttempt o1, TaskAttempt o2) {
                                                return o1.startTime < o2.startTime ? -1
                                                        : (o1.startTime > o2.startTime ? 1 : 0);
                                            }
                                        });
                                    }
                                }
                                Arrays.sort(tcAttempts, new Comparator<TaskClusterAttempt>() {
                                    @Override
                                    public int compare(TaskClusterAttempt o1, TaskClusterAttempt o2) {
                                        return o1.startTime < o2.startTime ? -1 : (o1.startTime > o2.startTime ? 1 : 0);
                                    }
                                });
                                tcList.add(tcAttempts);
                            }
                        }
                    }
                }
            }
        }

        Map<TaskAttemptId, TaskProfile> tpMap = new HashMap<TaskAttemptId, TaskProfile>();
        if (jrO.has("profile")) {
            JsonNode pO = jrO.get("profile");
            if (pO.has("joblets")) {
                JsonNode jobletsA = pO.get("joblets");
                for (int i = 0; i < jobletsA.size(); ++i) {
                    JsonNode jobletO = jobletsA.get(i);
                    if (jobletO.has("tasks")) {
                        JsonNode tasksA = jobletO.get("tasks");
                        for (int j = 0; j < tasksA.size(); ++j) {
                            JsonNode taskO = tasksA.get(j);
                            ActivityId activityId = ActivityId.parse(taskO.get("activity-id").asText());
                            int partition = taskO.get("partition").asInt();
                            int attempt = taskO.get("attempt").asInt();
                            TaskAttemptId taId = new TaskAttemptId(new TaskId(activityId, partition), attempt);
                            if (taskO.has("partition-send-profile")) {
                                JsonNode taskProfilesA = taskO.get("partition-send-profile");
                                for (int k = 0; k < taskProfilesA.size(); ++k) {
                                    JsonNode ppO = taskProfilesA.get(k);
                                    long openTime = ppO.get("open-time").asLong();
                                    long closeTime = ppO.get("close-time").asLong();
                                    int resolution = ppO.get("resolution").asInt();
                                    long offset = ppO.get("offset").asLong();
                                    JsonNode frameTimesA = ppO.get("frame-times");
                                    long[] frameTimes = new long[frameTimesA.size()];
                                    for (int l = 0; l < frameTimes.length; ++l) {
                                        frameTimes[l] = frameTimesA.get(l).asLong() + offset;
                                    }
                                    TaskProfile tp = new TaskProfile(taId, openTime, closeTime, frameTimes, resolution);
                                    if (!tpMap.containsKey(tp.taId)) {
                                        tpMap.put(tp.taId, tp);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if (!tcList.isEmpty()) {
            Collections.sort(tcList, new Comparator<TaskClusterAttempt[]>() {
                @Override
                public int compare(TaskClusterAttempt[] o1, TaskClusterAttempt[] o2) {
                    if (o1.length == 0) {
                        return o2.length == 0 ? 0 : -1;
                    } else if (o2.length == 0) {
                        return 1;
                    }
                    return o1[0].startTime < o2[0].startTime ? -1 : (o1[0].startTime > o2[0].startTime ? 1 : 0);
                }
            });
            long range = maxTime - minTime;

            double leftOffset = 20;

            int xWidth = 1024;
            double width = ((double) xWidth) / range;
            StringBuilder buffer = new StringBuilder();
            int y = 0;
            for (TaskClusterAttempt[] tcAttempts : tcList) {
                for (int i = 0; i < tcAttempts.length; ++i) {
                    TaskClusterAttempt tca = tcAttempts[i];
                    long startTime = tca.startTime - minTime;
                    long endTime = tca.endTime - minTime;
                    buffer.append("<rect x=\"").append(startTime * width + leftOffset).append("\" y=\"")
                            .append(y * (HEIGHT + 1)).append("\" width=\"").append(width * (endTime - startTime))
                            .append("\" height=\"").append(HEIGHT).append("\"/>\n");
                    buffer.append("<text x=\"").append(endTime * width + leftOffset + 20).append("\" y=\"")
                            .append(y * (HEIGHT + 1) + HEIGHT * 3 / 4).append("\">")
                            .append((endTime - startTime) + " ms").append("</text>\n");
                    ++y;
                    for (int j = 0; j < tca.tasks.length; ++j) {
                        TaskAttempt ta = tca.tasks[j];
                        long tStartTime = ta.startTime - minTime;
                        long tEndTime = ta.endTime - minTime;
                        buffer.append("<rect x=\"").append(tStartTime * width + leftOffset).append("\" y=\"")
                                .append(y * (HEIGHT + 1) + HEIGHT / 4).append("\" width=\"")
                                .append(width * (tEndTime - tStartTime)).append("\" height=\"").append(HEIGHT / 2)
                                .append("\" style=\"fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)\"/>\n");
                        buffer.append("<text x=\"").append(tEndTime * width + leftOffset + 20).append("\" y=\"")
                                .append(y * (HEIGHT + 1) + HEIGHT * 3 / 4).append("\">")
                                .append((tEndTime - tStartTime) + " ms (" + ta.name + ":" + ta.partition + ")")
                                .append("</text>\n");
                        TaskProfile tp = tpMap.get(ta.taId);
                        if (tp != null) {
                            for (int k = 0; k < tp.frameTimes.length; ++k) {
                                long taOpenTime = tp.openTime - minTime;
                                buffer.append("<rect x=\"")
                                        .append(taOpenTime * width + leftOffset)
                                        .append("\" y=\"")
                                        .append(y * (HEIGHT + 1) + HEIGHT / 4)
                                        .append("\" width=\"1\" height=\"")
                                        .append(HEIGHT / 2)
                                        .append("\" style=\"fill:rgb(255,0,0);stroke-width:1;stroke:rgb(255,0,0)\"/>\n");
                                for (int l = 0; l < tp.frameTimes.length; ++l) {
                                    long ft = tp.frameTimes[l];
                                    long ftn = l < tp.frameTimes.length - 1 ? tp.frameTimes[l + 1] : ft;
                                    long taNextTime = ft - minTime;
                                    long barWidth = ftn - ft;
                                    buffer.append("<rect x=\"")
                                            .append(taNextTime * width + leftOffset)
                                            .append("\" y=\"")
                                            .append(y * (HEIGHT + 1) + HEIGHT / 4)
                                            .append("\" width=\"")
                                            .append(barWidth == 0 ? 1 : (barWidth * width))
                                            .append("\" height=\"")
                                            .append(HEIGHT / 2)
                                            .append("\" style=\"fill:rgb(0,255,0);stroke-width:1;stroke:rgb(0,255,0)\"/>\n");
                                }
                                long taCloseTime = tp.closeTime - minTime;
                                buffer.append("<rect x=\"")
                                        .append(taCloseTime * width + leftOffset)
                                        .append("\" y=\"")
                                        .append(y * (HEIGHT + 1) + HEIGHT / 4)
                                        .append("\" width=\"1\" height=\"")
                                        .append(HEIGHT / 2)
                                        .append("\" style=\"fill:rgb(0,0,255);stroke-width:1;stroke:rgb(0,0,255)\"/>\n");
                            }
                        }
                        ++y;
                    }
                }
            }
            buffer.append("<rect x=\"").append(leftOffset).append("\" y=\"").append(0).append("\" width=\"").append(1)
                    .append("\" height=\"").append((y + 2) * (HEIGHT + 1)).append("\"/>\n");
            buffer.append("<rect x=\"").append(0).append("\" y=\"").append((y + 1) * (HEIGHT + 1))
                    .append("\" width=\"").append(xWidth + 2 * leftOffset).append("\" height=\"").append(1)
                    .append("\"/>\n");
            buffer.append("</svg>");
            Label markup = new Label("job-timeline",
                    "<svg version=\"1.1\"\nxmlns=\"http://www.w3.org/2000/svg\" width=\"" + (xWidth * 1.5)
                            + "\" height=\"" + ((y + 2) * (HEIGHT + 1)) + "\">\n" + buffer.toString());
            markup.setEscapeModelStrings(false);
            add(markup);
        } else {
            Label markup = new Label("job-timeline", "No information available yet");
            add(markup);
        }
    }

    private static class TaskAttempt {
        private TaskAttemptId taId;
        private long startTime;
        private long endTime;
        private String name;
        private int partition;

        public TaskAttempt(TaskAttemptId taId, long startTime, long endTime) {
            this.taId = taId;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    private static class TaskClusterAttempt {
        private String tcId;
        private int attempt;
        private long startTime;
        private long endTime;
        private TaskAttempt[] tasks;

        public TaskClusterAttempt(String tcId, int attempt, long startTime, long endTime) {
            this.tcId = tcId;
            this.attempt = attempt;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    private static class TaskProfile {
        private TaskAttemptId taId;
        private long openTime;
        private long closeTime;
        private long[] frameTimes;
        private int resolution;

        public TaskProfile(TaskAttemptId taId, long openTime, long closeTime, long[] frameTimes, int resolution) {
            this.taId = taId;
            this.openTime = openTime;
            this.closeTime = closeTime;
            this.frameTimes = frameTimes;
            this.resolution = resolution;
        }
    }
}
