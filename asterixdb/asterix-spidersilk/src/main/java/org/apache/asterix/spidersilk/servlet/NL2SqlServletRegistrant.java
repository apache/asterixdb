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
package org.apache.asterix.spidersilk.servlet;

import org.apache.asterix.api.http.IApiServerRegistrant;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.utils.Servlets;
import org.apache.hyracks.http.server.HttpServer;

/**
 * Registers the {@link NL2SqlServlet} on the JSON API server via the
 * {@link IApiServerRegistrant} ServiceLoader extension point.
 *
 * This class is discovered automatically at runtime through:
 * {@code META-INF/services/org.apache.asterix.api.http.IApiServerRegistrant}
 *
 * No modification to {@code CCApplication.java} is required beyond the
 * one-time addition of the ServiceLoader call in {@code setupJSONAPIServer()}.
 */
public class NL2SqlServletRegistrant implements IApiServerRegistrant {

    @Override
    public void register(ICcApplicationContext appCtx, HttpServer apiServer) {
        // The translator is null here; it will be initialized from configuration
        // in a follow-up phase when LangChain4j integration is added.
        apiServer.addServlet(new NL2SqlServlet(apiServer.ctx(), new String[] { Servlets.NL2SQL_SERVICE }, null));
    }
}
