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

import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;

public abstract class AbstractServlet implements IServlet {
    protected final String[] paths;
    protected final ConcurrentMap<String, Object> ctx;
    private final int[] trims;

    public AbstractServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        this.paths = paths;
        this.ctx = ctx;
        trims = new int[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            if (path.endsWith("/*")) {
                trims[i] = path.indexOf("/*");
            } else if (path.endsWith("/")) {
                trims[i] = path.length() - 1;
            } else {
                trims[i] = path.length();
            }
        }
    }

    @Override
    public String[] getPaths() {
        return paths;
    }

    @Override
    public ConcurrentMap<String, Object> ctx() {
        return ctx;
    }

    public String path(IServletRequest request) {
        int trim = -1;
        if (paths.length > 1) {
            for (int i = 0; i < paths.length; i++) {
                String path = paths[i].indexOf('*') >= 0 ? paths[i].substring(0, paths[i].indexOf('*')) : paths[0];
                if (request.getHttpRequest().uri().indexOf(path) == 0) {
                    trim = trims[i];
                    break;
                }
            }
        } else {
            trim = trims[0];
        }
        return request.getHttpRequest().uri().substring(trim);
    }
}
