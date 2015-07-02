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
package org.apache.hyracks.control.nc.work;

import java.io.ByteArrayOutputStream;

import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;

public class StateDumpWork extends SynchronizableWork {
    private final NodeControllerService ncs;

    private final String stateDumpId;

    public StateDumpWork(NodeControllerService ncs, String stateDumpId) {
        this.ncs = ncs;
        this.stateDumpId = stateDumpId;
    }

    @Override
    protected void doRun() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ncs.getApplicationContext().getStateDumpHandler().dumpState(baos);
        ncs.getClusterController().notifyStateDump(ncs.getApplicationContext().getNodeId(), stateDumpId,
                baos.toString("UTF-8"));
    }
}
