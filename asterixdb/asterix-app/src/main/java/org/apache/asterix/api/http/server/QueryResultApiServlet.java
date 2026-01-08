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
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.CreatedAtPrinter;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.app.result.fields.ProfilePrinter;
import org.apache.asterix.app.result.fields.ResultsPrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.ResultMetadata;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.asterix.utils.AsyncRequestsAPIUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.api.result.ResultSetId;
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
    protected void delete(IServletRequest request, IServletResponse response) throws Exception {
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
        final String strHandle = localPath(request);
        final ResultHandle handle = parseAndValidateHandle(request, response);
        if (handle == null) {
            return; // Response status already set in parseAndValidateHandle
        } else if (handle.getPartition() != -1) {
            // Can only discard entire result sets, not individual partitions
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        try {
            discardResult(handle.getRequestId(), handle.getJobId(), handle.getResultSetId());
            response.setStatus(HttpResponseStatus.ACCEPTED);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.NO_RESULT_SET)) {
                LOGGER.log(Level.INFO, "No results found for handle: \"{}\"", strHandle);
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            LOGGER.log(Level.WARN, "unexpected exception thrown from discard result", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "unexpected exception thrown from discard result", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    protected void discardResult(String requestId, JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        AsyncRequestsAPIUtil.discardResultPartitions((ICcApplicationContext) appCtx, jobId, resultSetId, requestId);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
        final String strHandle = localPath(request);
        final ResultHandle handle = parseAndValidateHandle(request, response);
        if (handle == null) {
            return; // Response status already set in parseAndValidateHandle
        }
        IResultSet resultSet = getResultSet();
        ResultReader resultReader = handle.getPartition() != -1
                ? new ResultReader(resultSet, handle.getJobId(), handle.getResultSetId(), handle.getPartition())
                : new ResultReader(resultSet, handle.getJobId(), handle.getResultSetId());
        try {
            ResultJobRecord.Status status = resultReader.getStatus();
            final HttpResponseStatus httpStatus = ResultUtil.getHttpStatusFromResultStatus(status);
            response.setStatus(httpStatus);
            if (httpStatus != HttpResponseStatus.OK) {
                return;
            }
            ResultMetadata metadata = (ResultMetadata) resultReader.getMetadata();
            SessionOutput sessionOutput = initResponse(request, response, metadata.getFormat());
            processResults(handle, resultReader, sessionOutput, metadata, request);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.NO_RESULT_SET)) {
                LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            LOGGER.log(Level.WARN, "Error retrieving result for handle: \"{}\"", handle, e);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            LOGGER.log(Level.WARN, "Error retrieving result for handle: \"{}\"", handle, e);
        }
        if (response.writer().checkError()) {
            LOGGER.warn("Error flushing output writer for \"{}\"", strHandle);
        }
    }

    private void processResults(ResultHandle handle, ResultReader resultReader, SessionOutput sessionOutput,
            ResultMetadata metadata, IServletRequest request) throws HyracksDataException {
        ResponsePrinter printer = new ResponsePrinter(sessionOutput);
        final Stats stats = new Stats();
        boolean printStarted = false;
        try {
            if (metadata.getFormat() == SessionConfig.OutputFormat.CLEAN_JSON
                    || metadata.getFormat() == SessionConfig.OutputFormat.LOSSLESS_JSON
                    || metadata.getFormat() == SessionConfig.OutputFormat.LOSSLESS_ADM_JSON) {
                printer.begin();
                printStarted = true;
                printer.addResultPrinter(new ResultsPrinter(appCtx, resultReader, null, stats, sessionOutput));
                printer.printResults();
                ResponseMetrics metrics = buildMetrics(stats, metadata);
                printer.addFooterPrinter(new MetricsPrinter(metrics, HttpUtil.getPreferredCharset(request)));
                if (metadata.getJobProfile() != null) {
                    printer.addFooterPrinter(new ProfilePrinter(metadata.getJobProfile()));
                }
                if (handle.getRequestId() != null) {
                    printer.addFooterPrinter(new CreatedAtPrinter(metadata.getCreateTime()));
                }
                printer.printFooters();
                printer.end();
                printStarted = false;
            } else {
                ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats, null);
            }
        } finally {
            if (printStarted) {
                printer.end();
            }
        }
    }

    private ResponseMetrics buildMetrics(Stats stats, ResultMetadata metadata) {
        long endTime = System.nanoTime();
        stats.setProcessedObjects(metadata.getProcessedObjects());
        stats.setQueueWaitTime(metadata.getQueueWaitTimeInNanos());
        stats.setBufferCacheHitRatio(metadata.getBufferCacheHitRatio());
        stats.setBufferCachePageReadCount(metadata.getBufferCachePageReadCount());
        stats.setCloudReadRequestsCount(metadata.getCloudReadRequestsCount());
        stats.setCloudPagesReadCount(metadata.getCloudPagesReadCount());
        stats.setCloudPagesPersistedCount(metadata.getCloudPagesPersistedCount());
        stats.updateTotalWarningsCount(metadata.getTotalWarningsCount());
        return ResponseMetrics.of(TimeUnit.MILLISECONDS.toNanos(endTime - metadata.getCreateTime()),
                metadata.getJobDuration(), stats.getCount(), stats.getSize(), metadata.getProcessedObjects(), 0,
                metadata.getTotalWarningsCount(), metadata.getCompileTime(), stats.getQueueWaitTime(),
                stats.getBufferCacheHitRatio(), stats.getBufferCachePageReadCount(), stats.getCloudReadRequestsCount(),
                stats.getCloudPagesReadCount(), stats.getCloudPagesPersistedCount());
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
        SessionConfig.HyracksJobFormat hyracksJobFormat =
                SessionConfig.HyracksJobFormat.get(request.getParameter("hyracks-job-format"), "hyracks-job-format",
                        SessionConfig.HyracksJobFormat.JSON, LOGGER);

        SessionOutput.ResultAppender appendHandle = (app, handle) -> app.append("{ \"").append("handle")
                .append("\":" + " \"").append(handle).append("\" }");
        SessionConfig sessionConfig = new SessionConfig(format, planFormat, hyracksJobFormat);

        // If it's JSON or ADM, check for the "wrapper-array" flag. Default is
        // "true" for JSON and "false" for ADM. (Not applicable for CSV.)
        boolean wrapperArray =
                format == SessionConfig.OutputFormat.CLEAN_JSON || format == SessionConfig.OutputFormat.LOSSLESS_JSON
                        || format == SessionConfig.OutputFormat.LOSSLESS_ADM_JSON;
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
            case LOSSLESS_ADM_JSON:
                // No need to reflect in output type; fall through
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
