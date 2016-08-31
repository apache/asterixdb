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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.asterix.clienthelper.Args;

public abstract class RemoteCommand extends ClientCommand {
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
        URL url = new URL("http://" + hostPort + "/" + path);
        try {
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod(method.name());
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
}
