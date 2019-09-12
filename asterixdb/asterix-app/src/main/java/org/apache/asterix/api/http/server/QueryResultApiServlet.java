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

import org.apache.asterix.api.common.ResultMetadata;
import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.app.result.fields.ProfilePrinter;
import org.apache.asterix.app.result.fields.ResultsPrinter;
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
        long elapsedStart = System.nanoTime();
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
            ResultMetadata metadata = (ResultMetadata) resultReader.getMetadata();
            SessionOutput sessionOutput = initResponse(request, response, metadata.getFormat());
            ResponsePrinter printer = new ResponsePrinter(sessionOutput);
            if (metadata.getFormat() == SessionConfig.OutputFormat.CLEAN_JSON
                    || metadata.getFormat() == SessionConfig.OutputFormat.LOSSLESS_JSON) {
                final Stats stats = new Stats();
                printer.begin();
                printer.addResultPrinter(new ResultsPrinter(appCtx, resultReader, null, stats, sessionOutput));
                printer.printResults();
                ResponseMetrics metrics = ResponseMetrics.of(System.nanoTime() - elapsedStart,
                        metadata.getJobDuration(), stats.getCount(), stats.getSize(), metadata.getProcessedObjects(), 0,
                        metadata.getTotalWarningsCount(), metadata.getDiskIoCount());
                printer.addFooterPrinter(new MetricsPrinter(metrics, HttpUtil.getPreferredCharset(request)));
                if (metadata.getJobProfile() != null) {
                    printer.addFooterPrinter(new ProfilePrinter(metadata.getJobProfile()));
                }
                printer.printFooters();
                printer.end();
            } else {
                ResultUtil.printResults(appCtx, resultReader, sessionOutput, new Stats(), null);
            }
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
    static SessionOutput initResponse(IServletRequest request, IServletResponse response,
            SessionConfig.OutputFormat format) throws IOException {
        String accept = request.getHeader("Accept");
        if (accept == null) {
            accept = "";
        }
        SessionConfig.PlanFormat planFormat = SessionConfig.PlanFormat.get(request.getParameter("plan-format"),
                "plan format", SessionConfig.PlanFormat.STRING, LOGGER);

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
        SessionOutput.ResultDecorator resultPrefix = null;
        SessionOutput.ResultDecorator resultPostfix = null;
        switch (format) {
            case ADM:
                HttpUtil.setContentType(response, "application/x-adm", request);
                break;
            case CLEAN_JSON:
                // No need to reflect "clean-ness" in output type; fall through
            case LOSSLESS_JSON:
                HttpUtil.setContentType(response, "application/json", request);
                resultPrefix = ResultUtil.createPreResultDecorator();
                resultPostfix = ResultUtil.createPostResultDecorator();
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
        return new SessionOutput(sessionConfig, response.writer(), resultPrefix, resultPostfix, appendHandle, null);
    }

}
