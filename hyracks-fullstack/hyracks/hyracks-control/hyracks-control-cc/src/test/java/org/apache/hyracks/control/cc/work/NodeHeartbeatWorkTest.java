/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.cc.work;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.junit.Test;

public class NodeHeartbeatWorkTest {

    private static final String NODE = "nc1";
    private static final InetSocketAddress NC_ADDRESS = new InetSocketAddress("127.0.0.1", 9115);

    private final IPCSystem clusterIpc = mock(IPCSystem.class);
    private final IIPCHandle ncHandle = mock(IIPCHandle.class);

    @Test
    public void unregisteredAuthorizedNodeIsToldToReregister() throws Exception {
        heartbeat(true);
        verify(ncHandle).send(anyLong(), argThat(
                f -> ((CCNCFunctions.NodeHeartbeatAckFunction) f).getException().matches(ErrorCode.NO_SUCH_NODE)),
                any());
    }

    @Test
    public void unauthorizedNodeIsNotAcked() throws Exception {
        heartbeat(false);
        verify(clusterIpc, never()).getReconnectingHandle(any());
        verify(ncHandle, never()).send(anyLong(), any(), any());
    }

    private void heartbeat(boolean authorized) throws Exception {
        ClusterControllerService ccs = mock(ClusterControllerService.class);
        INodeManager nodeManager = mock(INodeManager.class);
        ICCApplication app = mock(ICCApplication.class);
        when(ccs.getNodeManager()).thenReturn(nodeManager);
        when(ccs.getApplication()).thenReturn(app);
        when(app.getGatekeeper()).thenReturn(nodeId -> authorized);
        when(ccs.getCcId()).thenReturn(CcId.valueOf(0));
        when(ccs.getClusterIPC()).thenReturn(clusterIpc);
        when(clusterIpc.getReconnectingHandle(any())).thenReturn(ncHandle);
        new NodeHeartbeatWork(ccs, NODE, null, NC_ADDRESS).doRun();
    }
}
