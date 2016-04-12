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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class FileTestServer implements ITestServer {
    protected String[] paths;
    protected final int port;
    protected ServerSocket serverSocket;
    protected Thread listenerThread;

    public FileTestServer(int port) {
        this.port = port;
    }

    @Override
    public void configure(String[] args) {
        this.paths = args;
    }

    @Override
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        listenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!serverSocket.isClosed()) {
                    try (Socket socket = serverSocket.accept(); OutputStream os = socket.getOutputStream()) {
                        byte[] chunk = new byte[1024];
                        for (String path : paths) {
                            try (FileInputStream fin = new FileInputStream(new File(path))) {
                                int read = fin.read(chunk);
                                while (read > 0) {
                                    os.write(chunk, 0, read);
                                    read = fin.read(chunk);
                                }
                            }
                        }
                    } catch (IOException e) {
                        // Do nothing. This means the socket was closed for some reason.
                        // There is nothing to do here except try to close the socket and see if the
                        // server is still listening!
                        // This also could be due to the close() call
                    }
                }

            }
        });
        listenerThread.start();
    }

    @Override
    public void stop() throws IOException, InterruptedException {
        if (serverSocket.isBound()) {
            serverSocket.close();
            if (listenerThread.isAlive()) {
                listenerThread.join();
            }
        }
    }
}
