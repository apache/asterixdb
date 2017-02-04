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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.clienthelper.Args;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WaitForClusterCommand extends RemoteCommand {

    public WaitForClusterCommand(Args args) {
        super(args);
    }

    @Override
    @SuppressWarnings("squid:S2142") // interrupted exception
    public int execute() throws IOException {
        final int timeoutSecs = args.getTimeoutSecs();
        log("Waiting " + (timeoutSecs > 0 ? "up to " + timeoutSecs + " seconds " : "") + "for cluster " + hostPort
                + " to be available.");

        long startTime = System.currentTimeMillis();
        long timeoutMillis = TimeUnit.SECONDS.toMillis(timeoutSecs);
        boolean first = true;
        String lastState = null;
        while (true) {
            if (!first) {
                if (timeoutMillis > 0 && (startTime + timeoutMillis < System.currentTimeMillis())) {
                    break;
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return 22;
                }
            }
            first = false;

            HttpURLConnection conn;
            try {
                conn = openConnection(args.getClusterStatePath(), Method.GET);
                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    String result = IOUtils.toString(conn.getInputStream(), StandardCharsets.UTF_8.name());
                    ObjectMapper om = new ObjectMapper();
                    JsonNode json = om.readTree(result);
                    lastState = json.get("state").asText();
                    if ("ACTIVE".equals(lastState)) {
                        log("Cluster started and is ACTIVE.");
                        return 0;
                    }
                }
            } catch (IOException e) { //NOSONAR - log or rethrow exception
                // ignore exception, try again
            }
        }
        log("Cluster " + hostPort + " was not available before timeout of " + timeoutSecs + " seconds was exhausted"
                + (lastState != null ? " (state: " + lastState + ")" : "") + "; check logs for more information");
        return 1;
    }
}
