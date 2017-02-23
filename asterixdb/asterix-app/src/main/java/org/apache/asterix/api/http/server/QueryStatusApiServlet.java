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

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_DATASET_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.result.ResultReader;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryStatusApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryStatusApiServlet.class.getName());

    public QueryStatusApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        response.setStatus(HttpResponseStatus.OK);
        try {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure setting content type", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        String strHandle = request.getParameter("handle");
        PrintWriter out = response.writer();
        try {
            IHyracksDataset hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
            if (hds == null) {
                synchronized (ctx) {
                    hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
                    if (hds == null) {
                        hds = new HyracksDataset((IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR),
                                ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                        ctx.put(HYRACKS_DATASET_ATTR, hds);
                    }
                }
            }
            ObjectMapper om = new ObjectMapper();
            JsonNode handleObj = om.readTree(strHandle);
            JsonNode handle = handleObj.get("handle");
            JobId jobId = new JobId(handle.get(0).asLong());
            ResultSetId rsId = new ResultSetId(handle.get(1).asLong());

            /* TODO(madhusudancs): We need to find a way to LOSSLESS_JSON serialize default format obtained from
             * metadataProvider in the AQLTranslator and store it as part of the result handle.
             */
            ResultReader resultReader = new ResultReader(hds);
            resultReader.open(jobId, rsId);

            ObjectNode jsonResponse = om.createObjectNode();
            jsonResponse.put("status", resultReader.getStatus().name());
            out.write(jsonResponse.toString());

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure handling a request", e);
            out.println(e.getMessage());
            e.printStackTrace(out);
        }
    }

}
