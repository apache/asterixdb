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
package edu.uci.ics.hyracks.control.cc.remote.ops;

import java.util.UUID;

import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.control.cc.remote.RemoteOp;

public class JobCompleteNotifier implements RemoteOp<Void> {
    private String nodeId;
    private UUID jobId;
    private boolean status;

    public JobCompleteNotifier(String nodeId, UUID jobId, boolean status) {
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.status = status;
    }

    @Override
    public Void execute(INodeController node) throws Exception {
        node.cleanUpJob(jobId, status);
        return null;
    }

    @Override
    public String toString() {
        return jobId + " Cleaning Up";
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }
}