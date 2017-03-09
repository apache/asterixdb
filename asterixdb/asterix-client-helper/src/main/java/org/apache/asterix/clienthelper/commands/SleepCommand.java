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

import org.apache.asterix.clienthelper.Args;

public class SleepCommand extends ClientCommand {

    public SleepCommand(Args args) {
        super(args);

    }

    @Override
    public int execute() throws IOException {
        final int timeoutSecs = args.getTimeoutSecs();
        if (timeoutSecs <= 0) {
            throw new IllegalArgumentException("-timeout must be specified and greater than zero");
        }
        log("sleeping for " + timeoutSecs + " seconds...");
        try {
            Thread.sleep(timeoutSecs * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during sleep", e);
        }
        log("done!");
        return 0;
    }
}
