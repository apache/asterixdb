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
package edu.uci.ics.hyracks.api.job.statistics;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JobStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private Date startTime;

    private Date endTime;

    private List<StageStatistics> stages;

    public JobStatistics() {
        stages = new ArrayList<StageStatistics>();
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void addStageStatistics(StageStatistics stageStatistics) {
        stages.add(stageStatistics);
    }

    public List<StageStatistics> getStages() {
        return stages;
    }

    @Override
    public String toString() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        StringBuilder buffer = new StringBuilder();

        buffer.append("{\n");
        indent(buffer, 1).append("startTime: '").append(startTime == null ? null : df.format(startTime)).append("',\n");
        indent(buffer, 1).append("endTime: '").append(endTime == null ? null : df.format(endTime)).append("',\n");
        indent(buffer, 1).append("stages: [\n");
        boolean first = true;
        for (StageStatistics ss : stages) {
            if (!first) {
                buffer.append(",\n");
            }
            first = false;
            ss.toString(buffer, 2);
        }
        buffer.append("\n");
        indent(buffer, 1).append("]\n");
        buffer.append("}");

        return buffer.toString();
    }

    static StringBuilder indent(StringBuilder buffer, int level) {
        for (int i = 0; i < level; ++i) {
            buffer.append(" ");
        }
        return buffer;
    }
}