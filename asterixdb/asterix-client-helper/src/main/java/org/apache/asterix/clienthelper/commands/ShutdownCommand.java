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
package org.apache.asterix.clienthelper.commands;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.apache.asterix.clienthelper.Args;

public class ShutdownCommand extends RemoteCommand {
    private final String shutdownPath;

    public ShutdownCommand(Args args, String extra) {
        super(args);
        shutdownPath = args.getShutdownPath() + extra;
    }

    public ShutdownCommand(Args args) {
        this(args, "");
    }

    private void clusterLog(String suffix) {
        log("Cluster " + hostPort + " " + suffix);
    }

    @Override
    public int execute() throws IOException {
        log("Attempting to shutdown cluster " + hostPort + "...");
        int statusCode = tryPost(shutdownPath);
        // TODO (mblow): interrogate result to determine acceptance, not rely on HTTP 200
        switch (statusCode) {
            case HttpURLConnection.HTTP_ACCEPTED:
                clusterLog("accepted shutdown request.");
                return 0;
            case -1:
                clusterLog("not reachable.");
                return 1;
            default:
                clusterLog("shutdown request failed, with response code:" + statusCode);
        }
        return 1;

    }
}
