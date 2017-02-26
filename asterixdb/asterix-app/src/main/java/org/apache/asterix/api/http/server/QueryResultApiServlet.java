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

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryResultApiServlet extends AbstractQueryApiServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryResultApiServlet.class.getName());

    public QueryResultApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        response.setStatus(HttpResponseStatus.OK);
        // TODO this seems wrong ...
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
        String strHandle = request.getParameter("handle");
        PrintWriter out = response.writer();

        try {
            JsonNode handle = parseHandle(new ObjectMapper(), strHandle, LOGGER);
            if (handle == null) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return;
            }
            JobId jobId = new JobId(handle.get(0).asLong());
            ResultSetId rsId = new ResultSetId(handle.get(1).asLong());

            IHyracksDataset hds = getHyracksDataset();
            ResultReader resultReader = new ResultReader(hds, jobId, rsId);

            // QQQ The output format is determined by the initial
            // query and cannot be modified here, so calling back to
            // initResponse() is really an error. We need to find a
            // way to send the same OutputFormat value here as was
            // originally determined there. Need to save this value on
            // some object that we can obtain here.
            SessionConfig sessionConfig = RestApiServlet.initResponse(request, response);
            ResultUtil.printResults(resultReader, sessionConfig, new Stats(), null);
        } catch (HyracksDataException e) {
            final int errorCode = e.getErrorCode();
            if (ErrorCode.NO_RESULTSET == errorCode) {
                LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            out.println(e.getMessage());
            LOGGER.log(Level.WARNING, "Error retrieving result for \"" + strHandle + "\"", e);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            LOGGER.log(Level.WARNING, "Error retrieving result for \"" + strHandle + "\"", e);
        }
        if (out.checkError()) {
            LOGGER.warning("Error flushing output writer for \"" + strHandle + "\"");
        }
    }
}
