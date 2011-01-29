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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.control.cc.remote.Accumulator;

public class PortMapMergingAccumulator implements
        Accumulator<Map<PortInstanceId, Endpoint>, Map<PortInstanceId, Endpoint>> {
    Map<PortInstanceId, Endpoint> portMap = new HashMap<PortInstanceId, Endpoint>();

    @Override
    public void accumulate(Map<PortInstanceId, Endpoint> o) {
        portMap.putAll(o);
    }

    @Override
    public Map<PortInstanceId, Endpoint> getResult() {
        return portMap;
    }
}