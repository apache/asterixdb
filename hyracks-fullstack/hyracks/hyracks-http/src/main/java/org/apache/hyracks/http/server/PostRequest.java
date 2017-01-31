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
package org.apache.hyracks.http.server;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.util.ServletUtils;

import io.netty.handler.codec.http.FullHttpRequest;

public class PostRequest implements IServletRequest {
    private final FullHttpRequest request;
    private final List<String> names;
    private final List<String> values;
    private final Map<String, List<String>> parameters;

    public PostRequest(FullHttpRequest request, Map<String, List<String>> parameters, List<String> names,
            List<String> values) {
        this.request = request;
        this.parameters = parameters;
        this.names = names;
        this.values = values;
    }

    @Override
    public FullHttpRequest getHttpRequest() {
        return request;
    }

    @Override
    public String getParameter(CharSequence name) {
        for (int i = 0; i < names.size(); i++) {
            if (name.equals(names.get(i))) {
                return values.get(i);
            }
        }
        return ServletUtils.getParameter(parameters, name);
    }

    @Override
    public String getHeader(CharSequence name) {
        return request.headers().get(name);
    }
}
