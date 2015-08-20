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

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GetDatasetDirectoryServiceInfoWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final IResultCallback<NetworkAddress> callback;

    public GetDatasetDirectoryServiceInfoWork(ClusterControllerService ccs, IResultCallback<NetworkAddress> callback) {
        this.ccs = ccs;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            NetworkAddress addr = ccs.getDatasetDirectoryServiceInfo();
            callback.setValue(addr);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}