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

import org.json.JSONObject;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.work.GetActivityClusterGraphJSONWork;
import org.apache.hyracks.control.cc.work.GetJobRunJSONWork;
import org.apache.hyracks.control.cc.work.GetJobSummariesJSONWork;

public class JobsRESTAPIFunction implements IJSONOutputFunction {
    private ClusterControllerService ccs;

    public JobsRESTAPIFunction(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public JSONObject invoke(String[] arguments) throws Exception {
        JSONObject result = new JSONObject();
        switch (arguments.length) {
            case 1:
                if (!"".equals(arguments[0])) {
                    break;
                }
            case 0: {
                GetJobSummariesJSONWork gjse = new GetJobSummariesJSONWork(ccs);
                ccs.getWorkQueue().scheduleAndSync(gjse);
                result.put("result", gjse.getSummaries());
                break;
            }

            case 2: {
                JobId jobId = JobId.parse(arguments[0]);

                if ("job-activity-graph".equalsIgnoreCase(arguments[1])) {
                    GetActivityClusterGraphJSONWork gjage = new GetActivityClusterGraphJSONWork(ccs, jobId);
                    ccs.getWorkQueue().scheduleAndSync(gjage);
                    result.put("result", gjage.getJSON());
                } else if ("job-run".equalsIgnoreCase(arguments[1])) {
                    GetJobRunJSONWork gjre = new GetJobRunJSONWork(ccs, jobId);
                    ccs.getWorkQueue().scheduleAndSync(gjre);
                    result.put("result", gjre.getJSON());
                }

                break;
            }
        }
        return result;
    }
}