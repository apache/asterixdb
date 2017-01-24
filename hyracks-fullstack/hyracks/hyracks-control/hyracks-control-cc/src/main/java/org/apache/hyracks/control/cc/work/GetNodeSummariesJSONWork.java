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

import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.work.SynchronizableWork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class GetNodeSummariesJSONWork extends SynchronizableWork {
    private final INodeManager nodeManager;
    private ArrayNode summaries;

    public GetNodeSummariesJSONWork(INodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    protected void doRun() throws Exception {
        ObjectMapper om = new ObjectMapper();
        summaries = om.createArrayNode();
        for (NodeControllerState ncs : nodeManager.getAllNodeControllerStates()) {
            summaries.add(ncs.toSummaryJSON());
        }
    }

    public ArrayNode getSummaries() {
        return summaries;
    }
}
