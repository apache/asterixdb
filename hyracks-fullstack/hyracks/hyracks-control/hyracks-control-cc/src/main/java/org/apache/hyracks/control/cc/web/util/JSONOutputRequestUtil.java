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
package org.apache.hyracks.control.cc.web.util;

import java.net.URI;
import java.net.URISyntaxException;

public class JSONOutputRequestUtil {

    private JSONOutputRequestUtil() {
    }

    public static URI uri(String host, String prefix, String path) throws URISyntaxException {
        String name = host;
        int port = 80;
        int index = host.indexOf(':');
        if (index > 0) {
            String[] split = host.split(":");
            name = split[0];
            port = Integer.valueOf(split[1]);
        }
        return new URI("http", null, name, port, prefix + "/" + path, null, null);
    }
}
