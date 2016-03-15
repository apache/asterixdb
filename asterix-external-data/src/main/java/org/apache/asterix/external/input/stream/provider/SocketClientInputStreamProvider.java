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
package org.apache.asterix.external.input.stream.provider;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.stream.AInputStream;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Logger;

public class SocketClientInputStreamProvider implements IInputStreamProvider {

    private static final Logger LOGGER = Logger.getLogger(SocketClientInputStreamProvider.class.getName());
    private final Socket socket;

    public SocketClientInputStreamProvider(Pair<String, Integer> ipAndPort) throws HyracksDataException {
        try {
            socket = new Socket(ipAndPort.first, ipAndPort.second);
        } catch (IOException e) {
            LOGGER.error(
                    "Problem in creating socket against host " + ipAndPort.first + " on the port " + ipAndPort.second,
                    e);
            throw new HyracksDataException(e);
        }
    }

    @Override
    public AInputStream getInputStream() throws HyracksDataException {
        InputStream in;
        try {
            in = socket.getInputStream();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return new AInputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("method not supported. use read(byte[] buffer, int offset, int length) instead");
            }

            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return in.read(buffer, offset, length);
            }

            @Override
            public boolean stop() throws Exception {
                if (!socket.isClosed()) {
                    try {
                        in.close();
                    } finally {
                        socket.close();
                    }
                }
                return true;
            }

            @Override
            public boolean skipError() throws Exception {
                return false;
            }

            @Override
            public void setFeedLogManager(FeedLogManager logManager) {
            }

            @Override
            public void setController(AbstractFeedDataFlowController controller) {
            }
        };
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
    }
}
