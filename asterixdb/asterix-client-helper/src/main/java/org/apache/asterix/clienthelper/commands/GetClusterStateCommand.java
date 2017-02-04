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

import org.apache.asterix.clienthelper.Args;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GetClusterStateCommand extends RemoteCommand {

    public GetClusterStateCommand(Args args) {
        super(args);
    }

    private void logState(String state) {
        final String hostPort = args.getClusterAddress() + ":" + args.getClusterPort();
        log("Cluster " + hostPort + " is " + state + ".");
    }

    @Override
    public int execute() throws IOException {
        log("Attempting to determine state of cluster " + hostPort + "...");
        HttpURLConnection conn;
        // 0 = ACTIVE, 1 = DOWN, 2 = UNUSABLE, 3 = OTHER
        try {
            conn = openConnection(args.getClusterStatePath(), Method.GET);
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                String result = IOUtils.toString(conn.getInputStream(), StandardCharsets.UTF_8.name());
                ObjectMapper om = new ObjectMapper();
                JsonNode json = om.readTree(result);
                final String state = json.get("state").asText();
                logState(state);
                switch (state) {
                    case "ACTIVE":
                        return 0;
                    case "UNUSABLE":
                        return 2;
                    default:
                        return 3;
                }
            }
            logState("UNKNOWN (HTTP error code: " + conn.getResponseCode() + ")");
            return 3;
        } catch (IOException e) { // NOSONAR - log or rethrow exception
            logState("DOWN");
            return 1;
        }
    }
}
