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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.apache.asterix.clienthelper.Args;

public abstract class ClientCommand {

    public enum Command {
        GET_CLUSTER_STATE("Get state of cluster (errorcode 0 = ACTIVE, 1 = DOWN, 2 = UNUSABLE, 3 = OTHER)"),
        WAIT_FOR_CLUSTER("Wait for cluster to be ready (errorcode 0 = ACTIVE, non-zero = UNKNOWN)"),
        SHUTDOWN_CLUSTER("Instructs the cluster to shut down, leaving NCService processes intact"),
        SHUTDOWN_CLUSTER_ALL("Instructs the cluster to shut down, including NCService processes"),
        SLEEP("Sleep for specified -timeout seconds (-timeout must be set and greater than zero)");

        private final String usage;
        private static final Map<String, Command> nameMap = new HashMap<>();

        static {
            for (Command command : values()) {
                nameMap.put(command.name(), command);
            }
        }

        Command(String usage) {
            this.usage = usage;
        }

        public String usage() {
            return usage;
        }

        public static Command valueOfSafe(String name) {
            return nameMap.get(name.toUpperCase());
        }
    }

    protected final Args args;

    public ClientCommand(Args args) {
        this.args = args;
    }

    @SuppressWarnings("squid:S106")
    protected void log(Level severity, String message) {
        if (!args.isQuiet()) {
            System.out.println(severity + ": " + message);
        }
    }

    protected void log(String message) {
        log(Level.INFO, message);
    }

    public abstract int execute() throws IOException;
}
