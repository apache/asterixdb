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
package edu.uci.ics.hyracks.control.cc.web;

import java.util.UUID;

import org.json.JSONObject;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobProfileJSONEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobSpecificationJSONEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobSummariesJSONEvent;
import edu.uci.ics.hyracks.control.cc.web.util.IJSONOutputFunction;

public class RESTAPIFunction implements IJSONOutputFunction {
    private ClusterControllerService ccs;

    public RESTAPIFunction(ClusterControllerService ccs) {
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
                GetJobSummariesJSONEvent gjse = new GetJobSummariesJSONEvent(ccs);
                ccs.getJobQueue().scheduleAndSync(gjse);
                result.put("result", gjse.getSummaries());
                break;
            }

            case 2: {
                UUID jobId = UUID.fromString(arguments[0]);

                if ("spec".equalsIgnoreCase(arguments[1])) {
                    GetJobSpecificationJSONEvent gjse = new GetJobSpecificationJSONEvent(ccs, jobId);
                    ccs.getJobQueue().scheduleAndSync(gjse);
                    result.put("result", gjse.getSpecification());
                } else if ("profile".equalsIgnoreCase(arguments[1])) {
                    GetJobProfileJSONEvent gjpe = new GetJobProfileJSONEvent(ccs, jobId);
                    ccs.getJobQueue().scheduleAndSync(gjpe);
                    result.put("result", gjpe.getProfile());
                }

                break;
            }
        }
        return result;
    }
}