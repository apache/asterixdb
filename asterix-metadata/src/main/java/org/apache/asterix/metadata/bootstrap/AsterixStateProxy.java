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

package org.apache.asterix.metadata.bootstrap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.logging.Logger;

import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IMetadataNode;

/**
 * Contains Asterix distributed state such as the AsterixProperties.
 */
public class AsterixStateProxy implements IAsterixStateProxy {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AsterixStateProxy.class.getName());

    private IMetadataNode metadataNode;
    private static final IAsterixStateProxy cc = new AsterixStateProxy();

    public static IAsterixStateProxy registerRemoteObject() throws RemoteException {
        IAsterixStateProxy stub = (IAsterixStateProxy) UnicastRemoteObject.exportObject(cc, 0);
        LOGGER.info("Asterix Distributed State Proxy Bound");
        return stub;
    }

    public static void unregisterRemoteObject() throws RemoteException {
        UnicastRemoteObject.unexportObject(cc, true);
        LOGGER.info("Asterix Distributed State Proxy Unbound");
    }

    @Override
    public void setMetadataNode(IMetadataNode metadataNode) throws RemoteException {
        this.metadataNode = metadataNode;
    }

    @Override
    public IMetadataNode getMetadataNode() throws RemoteException {
        return this.metadataNode;
    }
}
