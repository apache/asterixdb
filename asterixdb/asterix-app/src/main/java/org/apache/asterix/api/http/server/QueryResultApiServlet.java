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

import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryResultApiServlet extends AbstractQueryApiServlet {
    private static final Logger LOGGER = LogManager.getLogger();

    public QueryResultApiServlet(ConcurrentMap<String, Object> ctx, IApplicationContext appCtx, String... paths) {
        super(appCtx, ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        // TODO this seems wrong ...
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
        PrintWriter out = response.writer();

        final String strHandle = localPath(request);
        final ResultHandle handle = ResultHandle.parse(strHandle);
        if (handle == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }

        IHyracksDataset hds = getHyracksDataset();
        ResultReader resultReader = new ResultReader(hds, handle.getJobId(), handle.getResultSetId());

        try {
            DatasetJobRecord.Status status = resultReader.getStatus();

            final HttpResponseStatus httpStatus;
            if (status == null) {
                httpStatus = HttpResponseStatus.NOT_FOUND;
            } else {
                switch (status.getState()) {
                    case SUCCESS:
                        httpStatus = HttpResponseStatus.OK;
                        break;
                    case RUNNING:
                    case IDLE:
                    case FAILED:
                        httpStatus = HttpResponseStatus.NOT_FOUND;
                        break;
                    default:
                        httpStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                        break;
                }
            }
            response.setStatus(httpStatus);
            if (httpStatus != HttpResponseStatus.OK) {
                return;
            }

            // QQQ The output format is determined by the initial
            // query and cannot be modified here, so calling back to
            // initResponse() is really an error. We need to find a
            // way to send the same OutputFormat value here as was
            // originally determined there. Need to save this value on
            // some object that we can obtain here.
            SessionOutput sessionOutput = RestApiServlet.initResponse(request, response);
            ResultUtil.printResults(appCtx, resultReader, sessionOutput, new Stats(), null);
        } catch (HyracksDataException e) {
            final int errorCode = e.getErrorCode();
            if (ErrorCode.NO_RESULT_SET == errorCode) {
                LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            out.println(e.getMessage());
            LOGGER.log(Level.WARN, "Error retrieving result for \"" + strHandle + "\"", e);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            LOGGER.log(Level.WARN, "Error retrieving result for \"" + strHandle + "\"", e);
        }
        if (out.checkError()) {
            LOGGER.warn("Error flushing output writer for \"" + strHandle + "\"");
        }
    }

}
