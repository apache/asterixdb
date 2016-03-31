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

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.work.AbstractWork;

public class NotifyStateDumpResponse extends AbstractWork {

    private final ClusterControllerService ccs;

    private final String stateDumpId;

    private final String nodeId;

    private final String state;

    public NotifyStateDumpResponse(ClusterControllerService ccs, String nodeId, String stateDumpId, String state) {
        this.ccs = ccs;
        this.stateDumpId = stateDumpId;
        this.nodeId = nodeId;
        this.state = state;
    }

    @Override
    public void run() {
        ccs.getStateDumpRun(stateDumpId).notifyStateDumpReceived(nodeId, state);
    }
}
