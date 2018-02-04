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

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CcConnection {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IClusterController ccs;
    private boolean registrationPending;
    private Exception registrationException;
    private NodeParameters nodeParameters;

    CcConnection(IClusterController ccs) {
        this.ccs = ccs;
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

    public synchronized CcId registerNode(NodeRegistration nodeRegistration) throws Exception {
        registrationPending = true;
        ccs.registerNode(nodeRegistration);
        while (registrationPending) {
            wait();
        }
        if (registrationException != null) {
            LOGGER.log(Level.WARN, "Registering with {} failed with exception", this, registrationException);
            throw registrationException;
        }
        return getCcId();
    }

    public IClusterController getClusterControllerService() {
        return ccs;
    }

    public NodeParameters getNodeParameters() {
        return nodeParameters;
    }
}
