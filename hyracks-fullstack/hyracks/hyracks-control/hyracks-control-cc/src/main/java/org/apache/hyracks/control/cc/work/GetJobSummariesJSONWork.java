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
package org.apache.hyracks.control.cc.work;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GetJobSummariesJSONWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private ArrayNode summaries;

    public GetJobSummariesJSONWork(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    protected void doRun() throws Exception {
        ObjectMapper om = new ObjectMapper();
        summaries = om.createArrayNode();
        populateJSON(ccs.getActiveRunMap().values());
        populateJSON(ccs.getRunMapArchive().values());
    }

    private void populateJSON(Collection<JobRun> jobRuns)  {
        ObjectMapper om = new ObjectMapper();
        for (JobRun run : jobRuns) {
            ObjectNode jo = om.createObjectNode();
            jo.put("type", "job-summary");
            jo.put("job-id", run.getJobId().toString());
            jo.put("create-time", run.getCreateTime());
            jo.put("start-time", run.getStartTime());
            jo.put("end-time", run.getEndTime());
            jo.put("status", run.getStatus().toString());
            summaries.add(jo);
        }
    }

    public ArrayNode getSummaries() {
        return summaries;
    }
}
