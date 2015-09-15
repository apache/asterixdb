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
package org.apache.asterix.api.http.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HyracksProperties {
    private final InputStream is;

    private final Properties properties;

    private static String HYRACKS_IP = "127.0.0.1";

    private static int HYRACKS_PORT = 1098;

    public HyracksProperties() throws IOException {
        is = HyracksProperties.class.getClassLoader().getResourceAsStream("hyracks-deployment.properties");
        properties = new Properties();
        properties.load(is);
    }

    public String getHyracksIPAddress() {
        String strIP = properties.getProperty("cc.ip");
        if (strIP == null) {
            strIP = HYRACKS_IP;
        }
        return strIP;
    }

    public int getHyracksPort() {
        String strPort = properties.getProperty("cc.port");
        int port = HYRACKS_PORT;
        if (strPort != null) {
            port = Integer.parseInt(strPort);
        }
        return port;
    }
}
