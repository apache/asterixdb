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
package org.apache.asterix.test.server;

import org.apache.asterix.test.client.ITestClient;
import org.apache.asterix.test.client.TestClientProvider;

public class TestClientServer implements ITestServer {

    // port of the server to connect to
    private final int port;
    private ITestClient client;

    public TestClientServer(int port) {
        this.port = port;
    }

    @Override
    public void configure(String[] args) throws Exception {
        client = TestClientProvider.createTestClient(args, port);
    }

    @Override
    public void start() throws Exception {
        client.start();
    }

    @Override
    public void stop() throws Exception {
        client.stop();
    }

}
