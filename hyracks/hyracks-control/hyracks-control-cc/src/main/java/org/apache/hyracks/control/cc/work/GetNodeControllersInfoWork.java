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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.common.work.IResultCallback;

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