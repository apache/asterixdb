
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
package org.apache.asterix.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

public class RMIClientFactory implements RMIClientSocketFactory, Serializable {

    private final boolean sslEnabled;

    public RMIClientFactory(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public Socket createSocket(String host, int port) throws IOException {
        final SocketFactory factory = sslEnabled ? SSLSocketFactory.getDefault() : SocketFactory.getDefault();
        return factory.createSocket(host, port);
    }
}
