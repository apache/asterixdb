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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.CreatedAtPrinter;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.app.result.fields.PartitionInfoPrinter;
import org.apache.asterix.app.result.fields.ResultCountPrinter;
import org.apache.asterix.app.result.fields.ResultHandlePrinter;
import org.apache.asterix.app.result.fields.StatusPrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.ResultMetadata;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryStatusApiServlet extends AbstractQueryApiServlet {
    private static final Logger LOGGER = LogManager.getLogger();

    public QueryStatusApiServlet(ConcurrentMap<String, Object> ctx, IApplicationContext appCtx, String... paths) {
        super(appCtx, ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        final String strHandle = localPath(request);
        final ResultHandle handle = parseAndValidateHandle(request, response);
        if (handle == null) {
            return; // Response status already set in parseAndValidateHandle
        }
        boolean uriMode = handle.getRequestId() != null;
        ResultReader resultReader = new ResultReader(getResultSet(), handle.getJobId(), handle.getResultSetId());
        final ResultJobRecord.Status resultReaderStatus = resultReader.getStatus();
        if (resultReaderStatus == null) {
            LOGGER.info("No results for: \"{}\"", strHandle);
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        ResultStatus resultStatus = resultStatus(resultReaderStatus);
        RequestExecutionState executionState = new RequestExecutionState();
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        // Always return 200 OK for valid status requests irrespective of the result status.
        response.setStatus(HttpResponseStatus.OK);
        final PrintWriter resultWriter = response.writer();
        SessionOutput sessionOutput = new SessionOutput(resultWriter);
        ResponsePrinter printer = new ResponsePrinter(sessionOutput);
        printer.begin();
        printer.addHeaderPrinter(new StatusPrinter(resultStatus));
        printer.printHeaders();
        switch (resultStatus) {
            case SUCCESS -> handleSuccessfulResult(request, strHandle, uriMode, printer, resultReader);
            case TIMEOUT -> handleTimeout(handle, executionState, printer, response);
            case FATAL, FAILED -> handleFailure(handle, executionState, printer, response, resultReaderStatus);
            case QUEUED, RUNNING -> {}
        }
        printer.printResults();
        if (uriMode) {
            printMetricsAndFooters(printer, resultReader, request);
        }
        printer.end();
        if (response.writer().checkError()) {
            LOGGER.warn("Error flushing output writer");
        }
    }

    private void handleSuccessfulResult(IServletRequest request, String strHandle, boolean uriMode,
            ResponsePrinter printer, ResultReader resultReader) throws HyracksDataException {
        String servletPath = servletPath(request).replace("status", "result");
        String resHandle;
        if (uriMode) {
            resHandle = servletPath + strHandle;
        } else {
            resHandle = "http://" + host(request) + servletPath + strHandle;
        }
        printer.addResultPrinter(new ResultHandlePrinter(resHandle));
        if (uriMode) {
            printer.addResultPrinter(new ResultCountPrinter(
                    ((ResultMetadata) (resultReader.getResultSetReader().getResultMetadata())).getResultCount()));
            printer.addResultPrinter(
                    new PartitionInfoPrinter(resultReader.getResultSetReader().getResultRecords(), resHandle));
        }
    }

    private void handleTimeout(ResultHandle handle, RequestExecutionState executionState, ResponsePrinter printer,
            IServletResponse response) {
        RuntimeDataException hde = new RuntimeDataException(ErrorCode.REQUEST_TIMEOUT);
        hde.addSuppressed(new TimeoutException());
        handleExecuteStatementException(hde, executionState, handle.getRequestId(), null, response);
        requestFailed(hde, printer, executionState);
    }

    private void handleFailure(ResultHandle handle, RequestExecutionState executionState, ResponsePrinter printer,
            IServletResponse response, ResultJobRecord.Status resultReaderStatus) {
        Exception ex = extractException(resultReaderStatus);
        if (ex != null) {
            handleExecuteStatementException(ex, executionState, handle.getRequestId(), null, response);
            requestFailed(ex, printer, executionState);
        }
    }

    ResultStatus resultStatus(ResultJobRecord.Status status) {
        switch (status.getState()) {
            case IDLE:
                return ResultStatus.QUEUED;
            case RUNNING:
                return ResultStatus.RUNNING;
            case SUCCESS:
                return ResultStatus.SUCCESS;
            case FAILED:
                return ResultStatus.FAILED;
            case TIMEOUT:
                return ResultStatus.TIMEOUT;
            default:
                return ResultStatus.FATAL;
        }
    }

    Exception extractException(ResultJobRecord.Status status) {
        switch (status.getState()) {
            case FAILED:
                List<Exception> exceptions = status.getExceptions();
                if (exceptions != null && !exceptions.isEmpty()) {
                    return exceptions.get(0);
                }
                return null;
            default:
                return null;
        }
    }

    private void printMetricsAndFooters(ResponsePrinter printer, ResultReader resultReader, IServletRequest request)
            throws HyracksDataException {
        ResultMetadata metadata = (ResultMetadata) resultReader.getMetadata();
        if (metadata != null) {
            final IStatementExecutor.Stats stats = new IStatementExecutor.Stats();
            stats.setProcessedObjects(metadata.getProcessedObjects());
            stats.setQueueWaitTime(metadata.getQueueWaitTimeInNanos());
            stats.setBufferCacheHitRatio(metadata.getBufferCacheHitRatio());
            stats.setBufferCachePageReadCount(metadata.getBufferCachePageReadCount());
            stats.setCloudReadRequestsCount(metadata.getCloudReadRequestsCount());
            stats.setCloudPagesReadCount(metadata.getCloudPagesReadCount());
            stats.setCloudPagesPersistedCount(metadata.getCloudPagesPersistedCount());
            stats.updateTotalWarningsCount(metadata.getTotalWarningsCount());
            long endTime = System.currentTimeMillis();
            ResponseMetrics metrics =
                    ResponseMetrics.of(endTime - metadata.getCreateTime(), metadata.getJobDuration(), stats.getCount(),
                            stats.getSize(), metadata.getProcessedObjects(), 0, metadata.getTotalWarningsCount(),
                            metadata.getCompileTime(), stats.getQueueWaitTime(), stats.getBufferCacheHitRatio(),
                            stats.getBufferCachePageReadCount(), stats.getCloudReadRequestsCount(),
                            stats.getCloudPagesReadCount(), stats.getCloudPagesPersistedCount());
            printer.addFooterPrinter(new MetricsPrinter(metrics, HttpUtil.getPreferredCharset(request),
                    Set.of(MetricsPrinter.Metrics.ELAPSED_TIME, MetricsPrinter.Metrics.EXECUTION_TIME,
                            MetricsPrinter.Metrics.QUEUE_WAIT_TIME, MetricsPrinter.Metrics.COMPILE_TIME,
                            MetricsPrinter.Metrics.WARNING_COUNT, MetricsPrinter.Metrics.ERROR_COUNT,
                            MetricsPrinter.Metrics.PROCESSED_OBJECTS_COUNT)));
            printer.addFooterPrinter(new CreatedAtPrinter(metadata.getCreateTime()));
        }
        printer.printFooters();
    }
}
