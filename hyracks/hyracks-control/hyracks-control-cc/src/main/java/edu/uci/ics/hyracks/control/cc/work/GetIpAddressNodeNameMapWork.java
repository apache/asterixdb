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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GetIpAddressNodeNameMapWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private Map<String, Set<String>> map;

    public GetIpAddressNodeNameMapWork(ClusterControllerService ccs, Map<String, Set<String>> map) {
        this.ccs = ccs;
        this.map = map;
    }

    @Override
    protected void doRun() throws Exception {
        map.putAll(ccs.getIpAddressNodeNameMap());
    }
}