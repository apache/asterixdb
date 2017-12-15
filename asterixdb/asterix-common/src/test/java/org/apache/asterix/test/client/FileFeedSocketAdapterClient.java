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
package org.apache.asterix.test.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileFeedSocketAdapterClient implements ITestClient {
    private final int port;
    private final int wait;
    private final String url;
    private Socket socket;
    private String path;
    private int batchSize;
    private int maxCount;
    private OutputStream out = null;
    static final Logger LOGGER = LogManager.getLogger();

    // expected args: url, source-file-path, max-count, batch-size, wait
    public FileFeedSocketAdapterClient(int port, String[] args) throws Exception {
        this.port = port;
        if (args.length != 5) {
            throw new Exception(
                    "Invalid arguments for FileFeedSocketAdapterClient. Expected arguments <url> <source-file-path> <max-count> <batch-size> <wait>");
        }
        this.url = args[0];
        this.path = args[1];
        this.maxCount = Integer.parseInt(args[2]);
        this.batchSize = Integer.parseInt(args[3]);
        this.wait = Integer.parseInt(args[4]);
    }

    @Override
    public void start() throws Exception {
        synchronized (this) {
            wait(wait);
        }
        try {
            socket = new Socket(url, port);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Problem in creating socket against host " + url + " on the port " + port, e);
            throw e;
        }

        int recordCount = 0;
        BufferedReader br = null;
        try {
            out = socket.getOutputStream();
            br = new BufferedReader(new FileReader(path));
            String nextRecord;
            while ((nextRecord = br.readLine()) != null) {
                ByteBuffer b = StandardCharsets.UTF_8.encode(nextRecord);
                if (wait >= 1 && recordCount % batchSize == 0) {
                    Thread.sleep(wait);
                }
                out.write(b.array(), 0, b.limit());
                recordCount++;
                LOGGER.log(Level.DEBUG, "One record filed into feed");
                if (recordCount == maxCount) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Problem in closing socket against host " + url + " on the port " + port);
            e.printStackTrace();
        }
    }
}
