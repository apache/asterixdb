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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.feed.watch.StatsSubscriber;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ActiveStatsApiServlet extends AbstractServlet {

    private static final Logger LOGGER = Logger.getLogger(ActiveStatsApiServlet.class.getName());
    private static final int DEFAULT_EXPIRE_TIME = 2000;
    private final ActiveLifecycleListener activeLifecycleListener;

    public ActiveStatsApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx) {
        super(ctx, paths);
        this.activeLifecycleListener = (ActiveLifecycleListener) appCtx.getActiveLifecycleListener();
    }

    private JsonNode constructNode(ObjectMapper om, IActiveEntityEventsListener eventListener, long currentTime,
            long ttl) throws InterruptedException, IOException {
        long statsTimeStamp = eventListener.getStatsTimeStamp();
        if (currentTime - statsTimeStamp > ttl) {
            StatsSubscriber subscriber = new StatsSubscriber(eventListener);
            // refresh
            eventListener.refreshStats(5000);
            subscriber.sync();
        }
        return om.readTree(eventListener.getStats());
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        // Obtain all feed status
        String localPath = localPath(request);
        int expireTime;
        IActiveEntityEventsListener[] listeners = activeLifecycleListener.getNotificationHandler().getEventListeners();
        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);
        ObjectNode resNode = om.createObjectNode();
        PrintWriter responseWriter = response.writer();
        try {
            response.setStatus(HttpResponseStatus.OK);
            if (localPath.length() == 0 || localPath.length() == 1) {
                expireTime = DEFAULT_EXPIRE_TIME;
            } else {
                expireTime = Integer.valueOf(localPath.substring(1));
            }
            long currentTime = System.currentTimeMillis();
            for (int iter1 = 0; iter1 < listeners.length; iter1++) {
                resNode.putPOJO(listeners[iter1].getEntityId().toString(),
                        constructNode(om, listeners[iter1], currentTime, expireTime));
            }
            // Construct Response
            responseWriter.write(om.writerWithDefaultPrettyPrinter().writeValueAsString(resNode));
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.toString());
        }
        responseWriter.flush();
    }
}
