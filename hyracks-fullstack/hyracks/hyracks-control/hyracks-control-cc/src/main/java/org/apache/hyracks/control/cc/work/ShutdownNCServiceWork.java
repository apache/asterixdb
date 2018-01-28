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

import static org.apache.hyracks.control.common.controllers.ServiceConstants.NC_SERVICE_MAGIC_COOKIE;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import org.apache.hyracks.control.common.controllers.ServiceConstants.ServiceCommand;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A work which is run at CC shutdown for each NC specified in the configuration file.
 * It contacts the NC service on each node and instructs it to terminate.
 */
public class ShutdownNCServiceWork extends SynchronizableWork {

    private static final Logger LOGGER = LogManager.getLogger();

    private final String ncHost;
    private final int ncPort;
    private final String ncId;

    public ShutdownNCServiceWork(String ncHost, int ncPort, String ncId) {
        this.ncHost = ncHost;
        this.ncPort = ncPort;
        this.ncId = ncId;
    }

    @Override
    public final void doRun() {
        LOGGER.info("Connecting to NC service '" + ncId + "' at " + ncHost + ":" + ncPort);
        try (Socket s = new Socket(ncHost, ncPort)) {
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.writeUTF(NC_SERVICE_MAGIC_COOKIE);
            oos.writeUTF(ServiceCommand.TERMINATE.name());
            oos.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failed to contact NC service '" + ncId + "' at " + ncHost + ":" + ncPort, e);
        }
    }
}
