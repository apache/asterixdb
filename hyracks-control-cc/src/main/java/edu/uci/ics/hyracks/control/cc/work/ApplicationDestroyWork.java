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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.remote.RemoteOp;
import edu.uci.ics.hyracks.control.cc.remote.RemoteRunner;
import edu.uci.ics.hyracks.control.cc.remote.ops.ApplicationDestroyer;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.FutureValue;

public class ApplicationDestroyWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final String appName;
    private FutureValue fv;

    public ApplicationDestroyWork(ClusterControllerService ccs, String appName, FutureValue fv) {
        this.ccs = ccs;
        this.appName = appName;
        this.fv = fv;
    }

    @Override
    public void run() {
        final ApplicationContext appCtx = ccs.getApplicationMap().remove(appName);
        if (appCtx == null) {
            fv.setException(new HyracksException("No application with name: " + appName));
            return;
        }
        List<RemoteOp<Void>> opList = new ArrayList<RemoteOp<Void>>();
        for (final String nodeId : ccs.getNodeMap().keySet()) {
            opList.add(new ApplicationDestroyer(nodeId, appName));
        }
        final RemoteOp[] ops = opList.toArray(new RemoteOp[opList.size()]);
        ccs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    RemoteRunner.runRemote(ccs, ops, null);
                } catch (Exception e) {
                    fv.setException(e);
                    return;
                }
                ccs.getJobQueue().schedule(new AbstractWork() {
                    @Override
                    public void run() {
                        try {
                            appCtx.deinitialize();
                        } catch (Exception e) {
                            fv.setException(e);
                        }
                        fv.setValue(null);
                    }
                });
            }
        });
    }
}