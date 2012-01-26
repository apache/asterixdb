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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class ApplicationDestroyWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final String appName;
    private IResultCallback<Object> callback;

    public ApplicationDestroyWork(ClusterControllerService ccs, String appName, IResultCallback<Object> callback) {
        this.ccs = ccs;
        this.appName = appName;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            final CCApplicationContext appCtx = ccs.getApplicationMap().remove(appName);
            if (appCtx == null) {
                callback.setException(new HyracksException("No application with name: " + appName));
                return;
            }
            if (appCtx.getStatus() == ApplicationStatus.IN_DEINITIALIZATION
                    || appCtx.getStatus() == ApplicationStatus.DEINITIALIZED) {
                return;
            }
            Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
            appCtx.getDeinitializationPendingNodeIds().addAll(nodeMap.keySet());
            appCtx.setStatus(ApplicationStatus.IN_DEINITIALIZATION);
            appCtx.setDeinitializationCallback(callback);
            for (String nodeId : ccs.getNodeMap().keySet()) {
                NodeControllerState nodeState = nodeMap.get(nodeId);
                final INodeController node = nodeState.getNodeController();
                node.destroyApplication(appName);
            }
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}