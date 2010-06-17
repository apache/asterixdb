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
package edu.uci.ics.hyracks.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;

import edu.uci.ics.hyracks.api.controller.IClusterController;
import edu.uci.ics.hyracks.api.controller.INodeController;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.config.CCConfig;
import edu.uci.ics.hyracks.job.JobManager;
import edu.uci.ics.hyracks.web.WebServer;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController {
    private static final long serialVersionUID = 1L;

    private CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final Map<String, INodeController> nodeRegistry;

    private WebServer webServer;

    private final JobManager jobManager;

    private final Executor taskExecutor;

    public ClusterControllerService(CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        nodeRegistry = new LinkedHashMap<String, INodeController>();
        jobManager = new JobManager(this);
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer(new Handler[] { getAdminConsoleHandler() });
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService");
        Registry registry = LocateRegistry.createRegistry(ccConfig.port);
        registry.rebind(IClusterController.class.getName(), this);
        webServer.setPort(ccConfig.httpPort);
        webServer.start();
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping ClusterControllerService");
        webServer.stop();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    @Override
    public UUID createJob(JobSpecification jobSpec) throws Exception {
        return jobManager.createJob(jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return jobManager.createJob(jobSpec, jobFlags);
    }

    @Override
    public void registerNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        synchronized (this) {
            if (nodeRegistry.containsKey(id)) {
                throw new Exception("Node with this name already registered.");
            }
            nodeRegistry.put(id, nodeController);
        }
        nodeController.notifyRegistration(this);
        LOGGER.log(Level.INFO, "Registered INodeController: id = " + id);
    }

    @Override
    public void unregisterNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        synchronized (this) {
            nodeRegistry.remove(id);
        }
        LOGGER.log(Level.INFO, "Unregistered INodeController");
    }

    @Override
    public INodeController lookupNode(String id) throws Exception {
        return nodeRegistry.get(id);
    }

    public Executor getExecutor() {
        return taskExecutor;
    }

    public synchronized void notifyJobComplete(final UUID jobId) {
        for (final INodeController nc : nodeRegistry.values()) {
            taskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        nc.cleanUpJob(jobId);
                    } catch (Exception e) {
                    }
                }

            });
        }
    }

    @Override
    public void notifyStageletComplete(UUID jobId, UUID stageId, String nodeId, StageletStatistics statistics)
            throws Exception {
        jobManager.notifyStageletComplete(jobId, stageId, nodeId, statistics);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return jobManager.getJobStatus(jobId);
    }

    @Override
    public void start(UUID jobId) throws Exception {
        jobManager.start(jobId);
    }

    @Override
    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        return jobManager.waitForCompletion(jobId);
    }

    private Handler getAdminConsoleHandler() {
        ContextHandler handler = new ContextHandler("/admin");
        handler.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException, ServletException {
                if (!"/".equals(target)) {
                    return;
                }
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                PrintWriter writer = response.getWriter();
                writer.println("<html><head><title>Hyracks Admin Console</title></head><body>");
                writer.println("<h1>Hyracks Admin Console</h1>");
                writer.println("<h2>Node Controllers</h2>");
                writer.println("<table><tr><td>Node Id</td><td>Host</td></tr>");
                synchronized (ClusterControllerService.this) {
                    for (Map.Entry<String, INodeController> e : nodeRegistry.entrySet()) {
                        try {
                            INodeController n = e.getValue();
                            writer.print("<tr><td>");
                            writer.print(e.getKey());
                            writer.print("</td><td>");
                            writer.print("</td></tr>");
                        } catch (Exception ex) {
                        }
                    }
                }
                writer.println("</table>");
                writer.println("</body></html>");
                writer.flush();
            }
        });
        return handler;
    }

    @Override
    public Map<String, INodeController> getRegistry() throws Exception {
        return nodeRegistry;
    }
}