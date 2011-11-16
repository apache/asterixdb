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
package edu.uci.ics.hyracks.control.cc.adminconsole.pages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.EmptyPanel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.util.string.StringValue;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.work.GetJobActivityGraphJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobRunJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobSpecificationJSONWork;

public class JobDetailsPage extends AbstractPage {
    private static final long serialVersionUID = 1L;

    private static final int HEIGHT = 9;
    private static final int WIDTH = 9;

    public JobDetailsPage(PageParameters params) throws Exception {
        ClusterControllerService ccs = getAdminConsoleApplication().getClusterControllerService();

        StringValue jobIdStr = params.get("job-id");

        JobId jobId = JobId.parse(jobIdStr.toString());

        GetJobSpecificationJSONWork gjsw = new GetJobSpecificationJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjsw);
        add(new Label("job-specification", gjsw.getJSON().toString()));

        GetJobActivityGraphJSONWork gjagw = new GetJobActivityGraphJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjagw);
        add(new Label("job-activity-graph", gjagw.getJSON().toString()));

        GetJobRunJSONWork gjrw = new GetJobRunJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjrw);
        add(new Label("job-run", gjrw.getJSON().toString()));

        JSONObject jrO = gjrw.getJSON();

        List<TaskProfile> taskProfiles = new ArrayList<TaskProfile>();
        if (jrO.has("profile")) {
            JSONObject pO = jrO.getJSONObject("profile");
            if (pO.has("joblets")) {
                JSONArray jobletsA = pO.getJSONArray("joblets");
                for (int i = 0; i < jobletsA.length(); ++i) {
                    JSONObject jobletO = jobletsA.getJSONObject(i);
                    if (jobletO.has("tasks")) {
                        JSONArray tasksA = jobletO.getJSONArray("tasks");
                        for (int j = 0; j < tasksA.length(); ++j) {
                            JSONObject taskO = tasksA.getJSONObject(j);
                            String activityId = taskO.getString("activity-id");
                            int partition = taskO.getInt("partition");
                            int attempt = taskO.getInt("attempt");
                            if (taskO.has("partition-send-profile")) {
                                JSONArray taskProfilesA = taskO.getJSONArray("partition-send-profile");
                                for (int k = 0; k < taskProfilesA.length(); ++k) {
                                    JSONObject ppO = taskProfilesA.getJSONObject(k);
                                    long openTime = ppO.getLong("open-time");
                                    long closeTime = ppO.getLong("close-time");
                                    JSONArray frameTimesA = ppO.getJSONArray("frame-times");
                                    long[] frameTimes = new long[frameTimesA.length()];
                                    for (int l = 0; l < frameTimes.length; ++l) {
                                        frameTimes[l] = frameTimesA.getLong(l);
                                    }
                                    taskProfiles.add(new TaskProfile(activityId, partition, attempt, openTime,
                                            closeTime, frameTimes));
                                }
                            }
                        }
                    }
                }
            }
        }
        if (!taskProfiles.isEmpty()) {
            Collections.sort(taskProfiles, new Comparator<TaskProfile>() {
                @Override
                public int compare(TaskProfile o1, TaskProfile o2) {
                    return o1.openTime < o2.openTime ? -1 : (o1.openTime > o2.openTime ? 1 : 0);
                }
            });
            long startTime = taskProfiles.get(0).openTime;
            long timeRange = taskProfiles.get(taskProfiles.size() - 1).closeTime - startTime;
            int n = taskProfiles.size();
            StringBuilder buffer = new StringBuilder();
            buffer.append("<svg viewBox=\"0 0 ").append((timeRange + 1) * (WIDTH + 1))
                    .append(' ').append((n + 1) * (HEIGHT + 1)).append("\" version=\"1.1\"\n");
            buffer.append("xmlns=\"http://www.w3.org/2000/svg\">\n");
            for (int i = 0; i < n; ++i) {
                TaskProfile tp = taskProfiles.get(i);
                open(buffer, i, tp.openTime - startTime);
                for (long ft : tp.frameTimes) {
                    nextFrame(buffer, i, ft - startTime);
                }
                close(buffer, i, tp.closeTime - startTime);
            }
            buffer.append("</svg>");
            Label markup = new Label("job-timeline", buffer.toString());
            markup.setEscapeModelStrings(false);
            add(markup);
        } else {
            add(new EmptyPanel("job-timeline"));
        }
    }

    private void open(StringBuilder buffer, int i, long openTime) {
        buffer.append("<rect x=\"").append(openTime * (WIDTH + 1)).append("\" y=\"").append(i * (HEIGHT + 1))
                .append("\" width=\"").append(WIDTH).append("\" height=\"").append(HEIGHT).append("\"/>\n");
    }

    private void close(StringBuilder buffer, int i, long closeTime) {
        buffer.append("<rect x=\"").append(closeTime * (WIDTH + 1)).append("\" y=\"").append(i * (HEIGHT + 1))
                .append("\" width=\"").append(WIDTH).append("\" height=\"").append(HEIGHT).append("\"/>\n");
    }

    private void nextFrame(StringBuilder buffer, int i, long frameTime) {
        buffer.append("<rect x=\"").append(frameTime * (WIDTH + 1)).append("\" y=\"").append(i * (HEIGHT + 1))
                .append("\" width=\"").append(WIDTH).append("\" height=\"").append(HEIGHT).append("\"/>\n");
    }

    private static class TaskProfile {
        private String activityId;
        private int partition;
        private int attempt;
        private long openTime;
        private long closeTime;
        private long[] frameTimes;

        public TaskProfile(String activityId, int partition, int attempt, long openTime, long closeTime,
                long[] frameTimes) {
            this.activityId = activityId;
            this.partition = partition;
            this.activityId = activityId;
            this.openTime = openTime;
            this.closeTime = closeTime;
            this.frameTimes = frameTimes;
        }
    }
}