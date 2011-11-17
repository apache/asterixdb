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
package edu.uci.ics.hyracks.api.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Connection Class used by a Hyracks Client to interact with a Hyracks Cluster
 * Controller using RMI. Usually, such a connection would be used when the CC
 * runs in a separate JVM from the client (The most common case).
 * 
 * @author vinayakb
 * 
 */
public final class HyracksRMIConnection extends AbstractHyracksConnection {
    /**
     * Constructor to create a connection to the Hyracks Cluster Controller.
     * 
     * @param host
     *            Host name (or IP Address) where the Cluster Controller can be
     *            reached.
     * @param port
     *            Port to reach the Hyracks Cluster Controller at the specified
     *            host name.
     * @throws Exception
     */
    public HyracksRMIConnection(String host, int port) throws Exception {
        super(host, lookupHCI(host, port));
    }

    private static IHyracksClientInterface lookupHCI(String host, int port) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(host, port);
        return (IHyracksClientInterface) registry.lookup(IHyracksClientInterface.class.getName());
    }
}