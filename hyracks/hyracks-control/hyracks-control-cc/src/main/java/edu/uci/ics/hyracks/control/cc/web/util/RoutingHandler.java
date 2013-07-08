/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.web.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class RoutingHandler extends AbstractHandler {
    private final Map<String, Handler> handlers;

    public RoutingHandler() {
        handlers = new HashMap<String, Handler>();
    }

    public synchronized void addHandler(String route, Handler handler) {
        handlers.put(route, handler);
    }

    public synchronized void removeHandler(String route) {
        handlers.remove(route);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        while (target.startsWith("/")) {
            target = target.substring(1);
        }
        int idx = target.indexOf('/');
        String path0 = target;
        String rest = "";
        if (idx >= 0) {
            path0 = target.substring(0, idx);
            rest = target.substring(idx);
        }
        Handler h;
        synchronized (this) {
            h = handlers.get(path0);
        }
        if (h != null) {
            h.handle(rest, baseRequest, request, response);
        }
    }
}