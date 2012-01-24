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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class ApplicationStartWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final String appName;
    private final IResultCallback<Object> callback;

    public ApplicationStartWork(ClusterControllerService ccs, String appName, IResultCallback<Object> callback) {
        this.ccs = ccs;
        this.appName = appName;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            final CCApplicationContext appCtx = ccs.getApplicationMap().get(appName);
            if (appCtx == null) {
                callback.setException(new HyracksException("No application with name: " + appName));
                return;
            }
            if (appCtx.getStatus() != ApplicationStatus.CREATED) {
                callback.setException(new HyracksException("Application in incorrect state for starting: "
                        + appCtx.getStatus()));
            }
            final Map<String, NodeControllerState> nodeMapCopy = new HashMap<String, NodeControllerState>(
                    ccs.getNodeMap());
            appCtx.getInitializationPendingNodeIds().addAll(nodeMapCopy.keySet());
            appCtx.setStatus(ApplicationStatus.IN_INITIALIZATION);
            appCtx.setInitializationCallback(callback);
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        appCtx.initializeClassPath();
                        appCtx.initialize();
                        final byte[] distributedState = JavaSerializationUtils.serialize(appCtx.getDistributedState());
                        final boolean deployHar = appCtx.containsHar();
                        for (final String nodeId : nodeMapCopy.keySet()) {
                            NodeControllerState nodeState = nodeMapCopy.get(nodeId);
                            final INodeController node = nodeState.getNodeController();
                            node.createApplication(appName, deployHar, distributedState);
                        }
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}