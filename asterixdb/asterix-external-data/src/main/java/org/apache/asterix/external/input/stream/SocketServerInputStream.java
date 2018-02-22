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
package org.apache.asterix.external.input.stream;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SocketServerInputStream extends AsterixInputStream {
    private static final Logger LOGGER = LogManager.getLogger();
    private ServerSocket server;
    private Socket socket;
    private InputStream connectionStream;

    public SocketServerInputStream(ServerSocket server) {
        this.server = server;
        socket = new Socket();
        connectionStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return -1;
            }
        };
    }

    @Override
    public int read() throws IOException {
        int read = connectionStream.read();
        while (read < 0) {
            accept();
            read = connectionStream.read();
        }
        return read;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (server == null) {
            return -1;
        }
        int read = -1;
        try {
            if (connectionStream.available() < 1) {
                controller.flush();
            }
            read = connectionStream.read(b, off, len);
        } catch (IOException e) {
            // exception is expected when no connection available
            LOGGER.info("Exhausted all pending connections. Waiting for new ones to come.");
            read = -1;
        }
        while (read < 0) {
            if (!accept()) {
                return -1;
            }
            try {
                read = connectionStream.read(b, off, len);
            } catch (IOException e) {
                e.printStackTrace();
                read = -1;
            }
        }
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        return 0;
    }

    @Override
    public int available() throws IOException {
        return 1;
    }

    @Override
    public synchronized void close() throws IOException {
        Throwable failure = CleanupUtils.close(connectionStream, null);
        connectionStream = null;
        failure = CleanupUtils.close(socket, failure);
        socket = null;
        failure = CleanupUtils.close(server, failure);
        server = null;
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private boolean accept() throws IOException {
        try {
            connectionStream.close();
            connectionStream = null;
            socket.close();
            socket = null;
            socket = server.accept();
            connectionStream = socket.getInputStream();
            return true;
        } catch (Exception e) {
            close();
            return false;
        }
    }

    @Override
    public boolean stop() throws Exception {
        close();
        return true;
    }

    @Override
    public boolean handleException(Throwable th) {
        try {
            return accept();
        } catch (IOException e) {
            LOGGER.warn("Failed accepting more connections", e);
            return false;
        }
    }
}
