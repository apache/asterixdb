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

package org.apache.asterix.experiment.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class SocketTweetGeneratorDriver {
    public static void main(String[] args) throws Exception {
        SocketTweetGeneratorConfig clientConfig = new SocketTweetGeneratorConfig();
        CmdLineParser clp = new CmdLineParser(clientConfig);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            clp.printUsage(System.err);
            System.exit(1);
        }

        if ((clientConfig.getDataInterval() == -1 && clientConfig.getDataGenDuration() == -1)
                || (clientConfig.getDataInterval() > 0 && clientConfig.getDataGenDuration() > 0)) {
            System.err.println("Must use exactly one of -d or -di");
            clp.printUsage(System.err);
            System.exit(1);
        }

        SocketTweetGenerator client = new SocketTweetGenerator(clientConfig);
        client.start();
    }
}
