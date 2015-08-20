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
package edu.uci.ics.hyracks.control.nc.runtime;

import java.util.Map;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class RootHyracksContext implements IHyracksRootContext {
    private final NodeControllerService ncs;

    private final IIOManager ioManager;

    public RootHyracksContext(NodeControllerService ncs, IIOManager ioManager) {
        this.ncs = ncs;
        this.ioManager = ioManager;
    }

    @Override
    public IIOManager getIOManager() {
        return ioManager;
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllerInfos() throws Exception {
        return ncs.getNodeControllersInfo();
    }
}