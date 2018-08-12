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
package org.apache.hyracks.control.nc;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CcConnection {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long REGISTRATION_RESPONSE_POLL_PERIOD = TimeUnit.SECONDS.toMillis(1);

    private final IClusterController ccs;
    private final InetSocketAddress ccAddress;
    private boolean registrationPending;
    private boolean registrationCompleted;
    private Exception registrationException;
    private NodeParameters nodeParameters;

    CcConnection(IClusterController ccs, InetSocketAddress ccAddress) {
        this.ccs = ccs;
        this.ccAddress = ccAddress;
    }

    @Override
    public String toString() {
        return ccs.toString();
    }

    public CcId getCcId() {
        return getNodeParameters().getClusterControllerInfo().getCcId();
    }

    synchronized void setNodeRegistrationResult(NodeParameters parameters, Exception exception) {
        nodeParameters = parameters;
        registrationException = exception;
        registrationPending = false;
        notifyAll();
    }

    public synchronized CcId registerNode(NodeRegistration nodeRegistration, int registrationId) throws Exception {
        registrationPending = true;
        ccs.registerNode(nodeRegistration, registrationId);
        try {
            InvokeUtil.runWithTimeout(() -> {
                this.wait(REGISTRATION_RESPONSE_POLL_PERIOD); // NOSONAR while loop in timeout call
            }, () -> !registrationPending, 1, TimeUnit.MINUTES);
        } catch (Exception e) {
            registrationException = e;
        }
        if (registrationException != null) {
            LOGGER.fatal("Registering with {} failed with exception", this, registrationException);
            ExitUtil.halt(ExitUtil.EC_NODE_REGISTRATION_FAILURE);
        }
        return getCcId();
    }

    public IClusterController getClusterControllerService() {
        return ccs;
    }

    public NodeParameters getNodeParameters() {
        return nodeParameters;
    }

    public synchronized void forceReregister(NodeControllerService ncs) throws InterruptedException {
        registrationCompleted = false;
        ncs.getExecutor().submit(() -> {
            try {
                return ncs.registerNode(this);
            } catch (Exception e) {
                LOGGER.log(Level.ERROR, "Failed registering with cc", e);
                throw new IllegalStateException(e);
            }
        });

        while (!registrationCompleted) {
            wait();
        }
    }

    public synchronized void notifyRegistrationCompleted() {
        registrationCompleted = true;
        notifyAll();
    }

    public InetSocketAddress getCcAddress() {
        return ccAddress;
    }
}
