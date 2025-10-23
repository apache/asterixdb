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

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.translator.ClientRequest;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class AbstractQueryApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final IApplicationContext appCtx;

    public enum ResultStatus {
        QUEUED("queued"),
        RUNNING("running"),
        SUCCESS("success"),
        TIMEOUT("timeout"),
        FAILED("failed"),
        FATAL("fatal");

        private final String str;

        ResultStatus(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    AbstractQueryApiServlet(IApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    protected IResultSet getResultSet() throws Exception { // NOSONAR
        return ServletUtil.getResultSet(appCtx, ctx);
    }

    protected IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        if (hcc == null) {
            throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
        }
        return hcc;
    }

    protected void handleExecuteStatementException(Throwable t,
            QueryServiceServlet.RequestExecutionState executionState, QueryServiceRequestParameters param,
            IServletResponse response) {
        handleExecuteStatementExceptionInternal(t, executionState, param.getRequestId(), param.getClientContextID(),
                param, response);
    }

    protected void handleExecuteStatementException(Throwable t,
            QueryServiceServlet.RequestExecutionState executionState, String requestId, String clientContextId,
            IServletResponse response) {
        handleExecuteStatementExceptionInternal(t, executionState, requestId, clientContextId, null, response);
    }

    private void handleExecuteStatementExceptionInternal(Throwable t,
            QueryServiceServlet.RequestExecutionState executionState, String requestId, String clientContextId,
            QueryServiceRequestParameters param, IServletResponse response) {
        if (t instanceof org.apache.asterix.lang.sqlpp.parser.TokenMgrError || t instanceof AlgebricksException) {
            if (LOGGER.isDebugEnabled()) {
                logException(Level.DEBUG, t.getMessage(), requestId, clientContextId, t);
            } else {
                logException(Level.INFO, t.getMessage(), requestId, clientContextId);
            }
            executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
            return;
        } else if (t instanceof IFormattedException) {
            IFormattedException formattedEx = (IFormattedException) t;
            Optional<IError> maybeError = formattedEx.getError();
            if (maybeError.isPresent()) {
                IError error = maybeError.get();
                boolean handled =
                        (param != null) ? handleIFormattedException(error, formattedEx, executionState, param, response)
                                : handleIFormattedException(error, formattedEx, executionState, requestId,
                                        clientContextId, response);
                if (handled) {
                    return;
                }
            }
        }
        logException(Level.WARN, "unexpected exception", requestId, clientContextId, t);
        executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    protected void logException(Level lvl, String msg, String clientCtxId, String uuid) {
        LOGGER.log(lvl, "handleException: {}: uuid={}, clientContextID={}", msg, uuid, clientCtxId);
    }

    protected void logException(Level lvl, String msg, String clientCtxId, String uuid, Throwable t) {
        LOGGER.log(lvl, "handleException: {}: uuid={}, clientContextID={}", msg, uuid, clientCtxId, t);
    }

    protected boolean handleIFormattedException(IError error, IFormattedException ex,
            QueryServiceServlet.RequestExecutionState executionState, QueryServiceRequestParameters param,
            IServletResponse response) {
        return handleIFormattedException(error, ex, executionState, param.getRequestId(), param.getClientContextID(),
                response);
    }

    protected boolean handleIFormattedException(IError error, IFormattedException ex,
            QueryServiceServlet.RequestExecutionState executionState, String requestId, String clientContextId,
            IServletResponse response) {
        if (ErrorCode.ASTERIX.equals(error.component())) {
            switch ((ErrorCode) error) {
                case INVALID_REQ_PARAM_VAL:
                case INVALID_REQ_JSON_VAL:
                case NO_STATEMENT_PROVIDED:
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
                    return true;
                case REQUEST_TIMEOUT:
                    logException(Level.INFO, "request execution timed out", requestId, clientContextId);
                    executionState.setStatus(ResultStatus.TIMEOUT, HttpResponseStatus.OK);
                    return true;
                case REQUEST_CANCELLED:
                    executionState.setStatus(ResultStatus.FAILED, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    return true;
                case REJECT_NODE_UNREGISTERED:
                case REJECT_BAD_CLUSTER_STATE:
                    logException(Level.WARN, ex.getMessage(), requestId, clientContextId);
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.SERVICE_UNAVAILABLE);
                    return true;
                default:
                    // fall-through
            }
        } else if (error instanceof org.apache.hyracks.api.exceptions.ErrorCode) {
            switch ((org.apache.hyracks.api.exceptions.ErrorCode) error) {
                case JOB_REQUIREMENTS_EXCEED_CAPACITY:
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
                    return true;
            }
        }
        return false;
    }

    protected void requestFailed(Throwable throwable, ResponsePrinter responsePrinter,
            QueryServiceServlet.RequestExecutionState executionState) {
        final ExecutionError executionError = ExecutionError.of(throwable);
        responsePrinter.addResultPrinter(new ErrorsPrinter(Collections.singletonList(executionError)));
    }

    protected ResultHandle parseAndValidateHandle(IServletRequest request, IServletResponse response) {
        final String strHandle = localPath(request);
        final ResultHandle handle = ResultHandle.parse(strHandle);
        if (handle == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return null;
        }
        try {
            if (handle.getRequestId() != null
                    && !isValidRequest(handle.getRequestId(), handle.getJobId(), request, response)) {
                return null;
            }
        } catch (HyracksDataException e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return null;
        }
        return handle;
    }

    protected boolean isValidRequest(String requestId, JobId jobId, IServletRequest request, IServletResponse response)
            throws HyracksDataException {
        Optional<IClientRequest> clientRequest =
                ((ICcApplicationContext) appCtx).getRequestTracker().getAsyncOrDeferredRequest(requestId);
        if (clientRequest.isEmpty() || ((ClientRequest) clientRequest.get()).getJobId() == null) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return false;
        }
        return true;
    }

    protected static class RequestExecutionState {
        private long execStart = -1;
        private long execEnd = -1;
        private ResultStatus resultStatus = ResultStatus.FATAL;
        private HttpResponseStatus httpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;

        public void setStatus(ResultStatus resultStatus, HttpResponseStatus httpResponseStatus) {
            this.resultStatus = resultStatus;
            this.httpResponseStatus = httpResponseStatus;
        }

        public ResultStatus getResultStatus() {
            return resultStatus;
        }

        public HttpResponseStatus getHttpStatus() {
            return httpResponseStatus;
        }

        void start() {
            execStart = System.nanoTime();
        }

        void end() {
            execEnd = System.nanoTime();
        }

        void finish() {
            if (execStart == -1) {
                execEnd = -1;
            } else if (execEnd == -1) {
                execEnd = System.nanoTime();
            }
        }

        public long duration() {
            return execEnd - execStart;
        }

        protected StringBuilder append(StringBuilder sb) {
            return sb.append("ResultStatus: ").append(resultStatus.str()).append(" HTTPStatus: ")
                    .append(String.valueOf(httpResponseStatus));
        }

        @Override
        public String toString() {
            return append(new StringBuilder()).toString();
        }
    }
}
