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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.clienthelper.Args;

public abstract class RemoteCommand extends ClientCommand {

    public static final int MAX_CONNECTION_TIMEOUT_SECS = 60;

    protected enum Method {
        GET,
        POST
    }

    protected final String hostPort;

    public RemoteCommand(Args args) {
        super(args);
        hostPort = args.getClusterAddress() + ":" + args.getClusterPort();
    }

    @SuppressWarnings("squid:S1166") // log or rethrow exception
    protected int tryConnect(String path, Method method) throws MalformedURLException {
        try {
            HttpURLConnection conn = openConnection(path, method);
            return conn.getResponseCode();
        } catch (IOException e) {
            return -1;
        }
    }

    protected int tryGet(String path) throws MalformedURLException {
        return tryConnect(path, Method.GET);
    }

    protected int tryPost(String path) throws MalformedURLException {
        return tryConnect(path, Method.POST);
    }

    protected InputStream openAsStream(String path, Method method) throws IOException {
        return openConnection(path, method).getInputStream();
    }

    protected HttpURLConnection openConnection(String path, Method method) throws IOException {
        URL url = new URL("http://" + hostPort + "/" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        final int timeoutMillis =
                (int) TimeUnit.SECONDS.toMillis(Math.max(MAX_CONNECTION_TIMEOUT_SECS, args.getTimeoutSecs()));
        conn.setConnectTimeout(timeoutMillis);
        conn.setReadTimeout(timeoutMillis);
        conn.setRequestMethod(method.name());
        conn.setRequestProperty("Connection", "close");
        return conn;
    }
}
