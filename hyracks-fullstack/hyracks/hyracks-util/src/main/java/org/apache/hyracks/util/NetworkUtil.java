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
package org.apache.hyracks.util;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private NetworkUtil() {
    }

    public static void configure(SocketChannel sc) throws IOException {
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    }

    public static void closeQuietly(SocketChannel sc) {
        if (sc.isOpen()) {
            try {
                sc.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close socket", e);
            }
        }
    }
}
