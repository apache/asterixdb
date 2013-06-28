/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.web;

import org.json.JSONObject;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.web.util.IJSONOutputFunction;
import edu.uci.ics.hyracks.control.cc.work.GetActivityClusterGraphJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobRunJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobSummariesJSONWork;

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