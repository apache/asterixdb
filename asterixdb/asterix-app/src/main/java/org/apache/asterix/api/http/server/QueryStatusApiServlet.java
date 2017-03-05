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
import java.io.StringWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.result.ResultReader;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryStatusApiServlet extends AbstractQueryApiServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryStatusApiServlet.class.getName());

    public QueryStatusApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        String strHandle = request.getParameter("handle");
        ObjectMapper om = new ObjectMapper();
        JsonNode handle = parseHandle(om, strHandle, LOGGER);
        if (handle == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        JobId jobId = new JobId(handle.get(0).asLong());
        ResultSetId rsId = new ResultSetId(handle.get(1).asLong());

        IHyracksDataset hds = getHyracksDataset();
        ResultReader resultReader = new ResultReader(hds, jobId, rsId);

        ResultStatus resultStatus = resultStatus(resultReader.getStatus());

        if (resultStatus == null) {
            LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }

        final StringWriter stringWriter = new StringWriter();
        final PrintWriter resultWriter = new PrintWriter(stringWriter);

        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        HttpResponseStatus httpStatus = HttpResponseStatus.OK;

        resultWriter.print("{\n");
        printStatus(resultWriter, resultStatus);

        if (ResultStatus.SUCCESS == resultStatus) {
            String servletPath = servletPath(request).replace("status", "result");
            String resHandle = "http://" + host(request) + servletPath + localPath(request);
            printHandle(resultWriter, resHandle);
        }

        resultWriter.print("}\n");
        resultWriter.flush();
        String result = stringWriter.toString();

        response.setStatus(httpStatus);
        response.writer().print(result);
        if (response.writer().checkError()) {
            LOGGER.warning("Error flushing output writer");
        }
    }

    ResultStatus resultStatus(DatasetJobRecord.Status status) {
        if (status == null) {
            return null;
        }
        switch (status) {
            case IDLE:
            case RUNNING:
                return ResultStatus.RUNNING;
            case SUCCESS:
                return ResultStatus.SUCCESS;
            case FAILED:
                return ResultStatus.FAILED;
            default:
                return ResultStatus.FATAL;
        }
    }
}
