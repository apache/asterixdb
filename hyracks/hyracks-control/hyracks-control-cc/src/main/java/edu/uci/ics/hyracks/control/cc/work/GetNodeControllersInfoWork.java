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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.client.NodeStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class GetNodeControllersInfoWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private IResultCallback<Map<String, NodeControllerInfo>> callback;

    public GetNodeControllersInfoWork(ClusterControllerService ccs,
            IResultCallback<Map<String, NodeControllerInfo>> callback) {
        this.ccs = ccs;
        this.callback = callback;
    }

    @Override
    public void run() {
        Map<String, NodeControllerInfo> result = new LinkedHashMap<String, NodeControllerInfo>();
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        for (Map.Entry<String, NodeControllerState> e : nodeMap.entrySet()) {
            result.put(e.getKey(), new NodeControllerInfo(e.getKey(), NodeStatus.ALIVE, e.getValue().getDataPort(), e
                    .getValue().getDatasetPort()));
        }
        callback.setValue(result);
    }
}