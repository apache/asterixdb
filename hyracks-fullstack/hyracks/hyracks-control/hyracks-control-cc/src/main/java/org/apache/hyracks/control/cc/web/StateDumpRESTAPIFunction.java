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

import java.util.Map;

import org.json.JSONObject;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.work.GatherStateDumpsWork;
import org.apache.hyracks.control.cc.work.GatherStateDumpsWork.StateDumpRun;

public class StateDumpRESTAPIFunction implements IJSONOutputFunction {
    private final ClusterControllerService ccs;

    public StateDumpRESTAPIFunction(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public JSONObject invoke(String[] arguments) throws Exception {
        GatherStateDumpsWork gsdw = new GatherStateDumpsWork(ccs);
        ccs.getWorkQueue().scheduleAndSync(gsdw);
        StateDumpRun sdr = gsdw.getStateDumpRun();
        sdr.waitForCompletion();

        JSONObject result = new JSONObject();
        for (Map.Entry<String, String> e : sdr.getStateDump().entrySet()) {
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

}
