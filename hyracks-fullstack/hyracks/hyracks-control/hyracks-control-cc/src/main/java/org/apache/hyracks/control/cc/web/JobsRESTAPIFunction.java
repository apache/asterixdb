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
package org.apache.hyracks.control.cc.web;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.web.util.JSONOutputRequestUtil;
import org.apache.hyracks.control.cc.work.GetActivityClusterGraphJSONWork;
import org.apache.hyracks.control.cc.work.GetJobRunJSONWork;
import org.apache.hyracks.control.cc.work.GetJobSummariesJSONWork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JobsRESTAPIFunction implements IJSONOutputFunction {

    private static final String[] DETAILS = new String[] { "job-run", "job-activity-graph" };

    private ClusterControllerService ccs;

    public JobsRESTAPIFunction(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public ObjectNode invoke(String host, String servletPath, String[] arguments) throws Exception {
        ObjectMapper om = new ObjectMapper();
        ObjectNode result = om.createObjectNode();
        switch (arguments.length) {
            case 1:
                if (!"".equals(arguments[0])) {
                    break;
                }
            case 0: {
                GetJobSummariesJSONWork gjse = new GetJobSummariesJSONWork(ccs.getJobManager());
                ccs.getWorkQueue().scheduleAndSync(gjse);
                result.set("result", enhanceSummaries(gjse.getSummaries(), host, servletPath));
                break;
            }

            case 2: {
                JobId jobId = JobId.parse(arguments[0]);

                if ("job-activity-graph".equalsIgnoreCase(arguments[1])) {
                    GetActivityClusterGraphJSONWork gjage = new GetActivityClusterGraphJSONWork(ccs, jobId);
                    ccs.getWorkQueue().scheduleAndSync(gjage);
                    result.set("result", gjage.getJSON());
                } else if ("job-run".equalsIgnoreCase(arguments[1])) {
                    GetJobRunJSONWork gjre = new GetJobRunJSONWork(ccs.getJobManager(), jobId);
                    ccs.getWorkQueue().scheduleAndSync(gjre);
                    result.set("result", gjre.getJSON());
                }

                break;
            }
        }
        return result;
    }

    private static ArrayNode enhanceSummaries(final ArrayNode summaries, String host, String servletPath)
            throws URISyntaxException {
        for (int i = 0; i < summaries.size(); ++i) {
            ObjectNode node = (ObjectNode) summaries.get(i);
            final String jid = node.get("job-id").asText();
            for (String field : DETAILS) {
                URI uri = JSONOutputRequestUtil.uri(host, servletPath, jid + "/" + field);
                node.put(field, uri.toString());
            }
        }
        return summaries;
    }
}
