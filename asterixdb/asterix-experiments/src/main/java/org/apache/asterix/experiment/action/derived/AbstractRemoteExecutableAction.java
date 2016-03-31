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

package org.apache.asterix.experiment.action.derived;

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

public abstract class AbstractRemoteExecutableAction extends AbstractExecutableAction {

    private final SSHClient client;

    private final String hostname;

    private final int port;

    private final String username;

    private final String keyLocation;

    private Command cmd;

    protected AbstractRemoteExecutableAction(String hostname, String username, String keyLocation) {
        this(hostname, SSHClient.DEFAULT_PORT, username, keyLocation);
    }

    protected AbstractRemoteExecutableAction(String hostname, int port, String username, String keyLocation) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.keyLocation = keyLocation;
        client = new SSHClient();
    }

    protected InputStream getErrorStream() {
        return cmd == null ? null : cmd.getErrorStream();
    }

    protected InputStream getInputStream() {
        return cmd == null ? null : cmd.getInputStream();
    }

    @Override
    protected boolean doExecute(String command, Map<String, String> env) throws Exception {
        int exitVal = 0;
        client.loadKnownHosts();
        try {
            client.connect(hostname, port);
            client.authPublickey(username, keyLocation);
            Session s = client.startSession();
            try {
                for (Entry<String, String> e : env.entrySet()) {
                    s.setEnvVar(e.getKey(), e.getValue());
                }
                cmd = s.exec(command);
                cmd.join();
                Integer ev = cmd.getExitStatus();
                exitVal = ev == null ? -1 : ev;
                cmd.close();
            } finally {
                s.close();
            }
        } finally {
            client.close();
        }
        return exitVal == 0;
    }
}
