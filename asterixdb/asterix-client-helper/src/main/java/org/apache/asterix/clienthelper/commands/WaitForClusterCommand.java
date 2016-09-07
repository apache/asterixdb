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

import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.clienthelper.Args;

public class WaitForClusterCommand extends RemoteCommand {

    public WaitForClusterCommand(Args args) {
        super(args);
    }

    @Override
    @SuppressWarnings("squid:S2142") // interrupted exception
    public int execute() throws IOException {
        log("Waiting "
                + (args.getTimeoutSecs() > 0 ? "up to " + args.getTimeoutSecs() + " seconds " : "")
                + "for cluster " + hostPort + " to be available.");

        long startTime = System.currentTimeMillis();
        while (true) {
            if (tryGet(args.getClusterStatePath()) == HttpServletResponse.SC_OK) {
                log("Cluster started.");
                return 0;
            }
            if (args.getTimeoutSecs() >= 0
                    && (startTime + (args.getTimeoutSecs() * 1000) < System.currentTimeMillis())) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                return 22;
            }
        }
        log("Cluster " + hostPort + " was not available before timeout of " + args.getTimeoutSecs()
                + " seconds was exhausted.");
        return 1;
    }
}
