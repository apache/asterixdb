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

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.web.util.IJSONOutputFunction;
import edu.uci.ics.hyracks.control.cc.work.GetNodeDetailsJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetNodeSummariesJSONWork;

public class NodesRESTAPIFunction implements IJSONOutputFunction {
    private ClusterControllerService ccs;

    public NodesRESTAPIFunction(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public JSONObject invoke(String[] arguments) throws Exception {
        JSONObject result = new JSONObject();
        switch (arguments.length) {
            case 1: {
                if ("".equals(arguments[0])) {
                    GetNodeSummariesJSONWork gnse = new GetNodeSummariesJSONWork(ccs);
                    ccs.getWorkQueue().scheduleAndSync(gnse);
                    result.put("result", gnse.getSummaries());
                } else {
                    String nodeId = arguments[0];
                    GetNodeDetailsJSONWork gnde = new GetNodeDetailsJSONWork(ccs, nodeId);
                    ccs.getWorkQueue().scheduleAndSync(gnde);
                    result.put("result", gnde.getDetail());
                }
            }
        }
        return result;
    }
}