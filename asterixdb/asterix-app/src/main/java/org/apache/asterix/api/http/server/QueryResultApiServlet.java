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
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultJobRecord;
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
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);

        final String strHandle = localPath(request);
        final ResultHandle handle = ResultHandle.parse(strHandle);
        if (handle == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }

        IResultSet resultSet = getResultSet();
        ResultReader resultReader = new ResultReader(resultSet, handle.getJobId(), handle.getResultSetId());

        try {
            ResultJobRecord.Status status = resultReader.getStatus();

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
            SessionOutput sessionOutput = initResponse(request, response);
            ResultUtil.printResults(appCtx, resultReader, sessionOutput, new Stats(), null);
        } catch (HyracksDataException e) {
            final int errorCode = e.getErrorCode();
            if (ErrorCode.NO_RESULT_SET == errorCode) {
                LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            response.writer().println(e.getMessage());
            LOGGER.log(Level.WARN, "Error retrieving result for \"" + strHandle + "\"", e);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            LOGGER.log(Level.WARN, "Error retrieving result for \"" + strHandle + "\"", e);
        }
        if (response.writer().checkError()) {
            LOGGER.warn("Error flushing output writer for \"" + strHandle + "\"");
        }
    }

    /**
     * Initialize the Content-Type of the response, and construct a
     * SessionConfig with the appropriate output writer and output-format
     * based on the Accept: header and other servlet parameters.
     */
    static SessionOutput initResponse(IServletRequest request, IServletResponse response) throws IOException {
        // CLEAN_JSON output is the default; most generally useful for a
        // programmatic HTTP API
        SessionConfig.OutputFormat format = SessionConfig.OutputFormat.CLEAN_JSON;
        // First check the "output" servlet parameter.
        String output = request.getParameter("output");
        String accept = request.getHeader("Accept", "");
        if (output != null) {
            if ("CSV".equals(output)) {
                format = SessionConfig.OutputFormat.CSV;
            } else if ("ADM".equals(output)) {
                format = SessionConfig.OutputFormat.ADM;
            }
        } else {
            // Second check the Accept: HTTP header.
            if (accept.contains("application/x-adm")) {
                format = SessionConfig.OutputFormat.ADM;
            } else if (accept.contains("text/csv")) {
                format = SessionConfig.OutputFormat.CSV;
            }
        }
        SessionConfig.PlanFormat planFormat = SessionConfig.PlanFormat.get(request.getParameter("plan-format"),
                "plan format", SessionConfig.PlanFormat.STRING, LOGGER);

        // If it's JSON, check for the "lossless" flag

        if (format == SessionConfig.OutputFormat.CLEAN_JSON
                && ("true".equals(request.getParameter("lossless")) || accept.contains("lossless=true"))) {
            format = SessionConfig.OutputFormat.LOSSLESS_JSON;
        }

        SessionOutput.ResultAppender appendHandle = (app, handle) -> app.append("{ \"").append("handle")
                .append("\":" + " \"").append(handle).append("\" }");
        SessionConfig sessionConfig = new SessionConfig(format, planFormat);

        // If it's JSON or ADM, check for the "wrapper-array" flag. Default is
        // "true" for JSON and "false" for ADM. (Not applicable for CSV.)
        boolean wrapperArray =
                format == SessionConfig.OutputFormat.CLEAN_JSON || format == SessionConfig.OutputFormat.LOSSLESS_JSON;
        String wrapperParam = request.getParameter("wrapper-array");
        if (wrapperParam != null) {
            wrapperArray = Boolean.valueOf(wrapperParam);
        } else if (accept.contains("wrap-array=true")) {
            wrapperArray = true;
        } else if (accept.contains("wrap-array=false")) {
            wrapperArray = false;
        }
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, wrapperArray);
        // Now that format is set, output the content-type
        switch (format) {
            case ADM:
                HttpUtil.setContentType(response, "application/x-adm", request);
                break;
            case CLEAN_JSON:
                // No need to reflect "clean-ness" in output type; fall through
            case LOSSLESS_JSON:
                HttpUtil.setContentType(response, "application/json", request);
                break;
            case CSV:
                // Check for header parameter or in Accept:.
                if ("present".equals(request.getParameter("header")) || accept.contains("header=present")) {
                    HttpUtil.setContentType(response, "text/csv; header=present", request);
                    sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, true);
                } else {
                    HttpUtil.setContentType(response, "text/csv; header=absent", request);
                }
                break;
            default:
                throw new IOException("Unknown format " + format);
        }
        return new SessionOutput(sessionConfig, response.writer(), null, null, appendHandle, null);
    }

}
