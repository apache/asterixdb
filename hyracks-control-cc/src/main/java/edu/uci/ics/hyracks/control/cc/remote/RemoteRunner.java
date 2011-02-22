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
package edu.uci.ics.hyracks.control.cc.remote;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.base.INodeController;

public class RemoteRunner {
    public static <T, R> R runRemote(ClusterControllerService ccs, final RemoteOp<T>[] remoteOps,
            final Accumulator<T, R> accumulator) throws Exception {
        final Semaphore installComplete = new Semaphore(remoteOps.length);
        final List<Exception> errors = new Vector<Exception>();
        for (final RemoteOp<T> remoteOp : remoteOps) {
            NodeControllerState nodeState = ccs.getNodeMap().get(remoteOp.getNodeId());
            final INodeController node = nodeState.getNodeController();

            installComplete.acquire();
            Runnable remoteRunner = new Runnable() {
                @Override
                public void run() {
                    try {
                        T t = remoteOp.execute(node);
                        if (accumulator != null) {
                            synchronized (accumulator) {
                                accumulator.accumulate(t);
                            }
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        installComplete.release();
                    }
                }
            };

            ccs.getExecutor().execute(remoteRunner);
        }
        installComplete.acquire(remoteOps.length);
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
        return accumulator == null ? null : accumulator.getResult();
    }
}
