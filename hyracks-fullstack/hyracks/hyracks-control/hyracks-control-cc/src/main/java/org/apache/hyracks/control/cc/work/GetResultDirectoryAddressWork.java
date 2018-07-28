/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GetResultDirectoryAddressWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final IResultCallback<NetworkAddress> callback;

    public GetResultDirectoryAddressWork(ClusterControllerService ccs, IResultCallback<NetworkAddress> callback) {
        this.ccs = ccs;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            NetworkAddress addr = ccs.getResultDirectoryAddress();
            callback.setValue(addr);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
