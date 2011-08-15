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
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.jobqueue.SynchronizableEvent;

public class AdminConsoleHandler extends AbstractHandler {
    private ClusterControllerService ccs;

    public AdminConsoleHandler(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if (!"/".equals(target)) {
            return;
        }
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        final PrintWriter writer = response.getWriter();
        writer.println("<html><head><title>Hyracks Admin Console</title></head><body>");
        writer.println("<h1>Hyracks Admin Console</h1>");
        writer.println("<h2>Node Controllers</h2>");
        writer.println("<table><tr><td>Node Id</td><td>Host</td></tr>");
        try {
            ccs.getJobQueue().scheduleAndSync(new SynchronizableEvent() {
                @Override
                protected void doRun() throws Exception {
                    for (Map.Entry<String, NodeControllerState> e : ccs.getNodeMap().entrySet()) {
                        try {
                            writer.print("<tr><td>");
                            writer.print(e.getKey());
                            writer.print("</td><td>");
                            writer.print(e.getValue().getLastHeartbeatDuration());
                            writer.print("</td></tr>");
                        } catch (Exception ex) {
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new IOException(e);
        }
        writer.println("</table>");
        writer.println("</body></html>");
        writer.flush();
    }
}