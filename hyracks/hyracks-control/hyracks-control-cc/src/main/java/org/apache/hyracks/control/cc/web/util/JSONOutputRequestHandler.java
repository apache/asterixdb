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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.JSONObject;

public class JSONOutputRequestHandler extends AbstractHandler {
    private final IJSONOutputFunction fn;

    public JSONOutputRequestHandler(IJSONOutputFunction fn) {
        this.fn = fn;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        while (target.startsWith("/")) {
            target = target.substring(1);
        }
        while (target.endsWith("/")) {
            target = target.substring(0, target.length() - 1);
        }
        String[] parts = target.split("/");
        try {
            JSONObject result = fn.invoke(parts);
            response.setContentType("application/json");
            result.write(response.getWriter());
            baseRequest.setHandled(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}