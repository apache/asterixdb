/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.web;

import java.io.IOException;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;

public class AdminConsoleHandler extends AbstractHandler {
    public AdminConsoleHandler(ClusterControllerService ccs) {
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            response.getWriter().println("No Administration Console found.");
        }

        URL url = new URL(request.getRequestURL().toString());
        String ccLoc = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort();
        response.sendRedirect("/console?cclocation=" + ccLoc);
    }
}