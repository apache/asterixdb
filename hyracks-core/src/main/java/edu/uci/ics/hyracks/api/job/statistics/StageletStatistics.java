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
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class StageletStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private String nodeId;

    private Date startTime;

    private Date endTime;

    private Map<String, String> statisticsMap;

    public StageletStatistics() {
        statisticsMap = Collections.synchronizedMap(new TreeMap<String, String>());
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
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

    public Map<String, String> getStatisticsMap() {
        return statisticsMap;
    }

    public void toString(StringBuilder buffer, int level) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        buffer.append("{\n");
        JobStatistics.indent(buffer, level + 1).append("nodeId: '").append(nodeId).append("',\n");
        JobStatistics.indent(buffer, level + 1).append("startTime: '").append(df.format(startTime)).append("',\n");
        JobStatistics.indent(buffer, level + 1).append("endTime: '").append(df.format(endTime)).append("',\n");
        JobStatistics.indent(buffer, level + 1).append("statistics: {\n");
        boolean first = true;
        for (Map.Entry<String, String> e : statisticsMap.entrySet()) {
            if (!first) {
                buffer.append(",\n");
            }
            first = false;
            JobStatistics.indent(buffer, level + 2).append(e.getKey()).append(": '").append(e.getValue()).append("'");
        }
        buffer.append("\n");
        JobStatistics.indent(buffer, level + 1).append("}\n");
        JobStatistics.indent(buffer, level).append("}");
    }
}