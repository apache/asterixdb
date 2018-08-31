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
package org.apache.hyracks.control.nc.heartbeat;

import java.net.InetSocketAddress;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.nc.CcConnection;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeartbeatManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private final NodeControllerService ncs;
    private final CcConnection ccc;
    private final Thread hbThread;
    private final CcId ccId;

    private HeartbeatManager(NodeControllerService ncs, CcConnection ccc, HeartbeatData hbData,
            InetSocketAddress ncAddress) {
        this.ncs = ncs;
        this.ccc = ccc;
        hbThread = new Thread(new HeartbeatTask(ncs.getId(), hbData, ccc.getClusterControllerService(),
                ccc.getNodeParameters().getHeartbeatPeriod(), ncAddress), ncs.getId() + "-Heartbeat");
        hbThread.setPriority(Thread.MAX_PRIORITY);
        hbThread.setDaemon(true);
        ccId = ccc.getCcId();
    }

    public static HeartbeatManager init(NodeControllerService ncs, CcConnection ccc, HeartbeatData hbData,
            InetSocketAddress ncAddress) {
        HeartbeatManager hbMgr = new HeartbeatManager(ncs, ccc, hbData, ncAddress);
        hbMgr.start();
        return hbMgr;
    }

    public void shutdown() {
        hbThread.interrupt();
    }

    public void start() {
        hbThread.start();
    }

    public void notifyAck(HyracksDataException exception) {
        LOGGER.debug("ack rec'd from {} w/ exception: {}", ccId::toString, () -> String.valueOf(exception));
        if (exception != null && exception.matches(ErrorCode.HYRACKS, ErrorCode.NO_SUCH_NODE)) {
            LOGGER.info("{} indicates it does not recognize us; force a reconnect", ccId);
            try {
                ccc.forceReregister(ncs);
            } catch (Exception e) {
                LOGGER.warn("ignoring exception attempting to reregister with {}", ccId, e);
            }
        }
    }
}
