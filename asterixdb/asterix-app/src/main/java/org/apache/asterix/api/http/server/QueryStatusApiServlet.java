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

import static org.apache.asterix.api.http.server.AbstractQueryApiServlet.ResultStatus.FAILED;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.app.result.fields.ResultHandlePrinter;
import org.apache.asterix.app.result.fields.StatusPrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
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
        final ResultHandle handle = ResultHandle.parse(strHandle);
        if (handle == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        ResultReader resultReader = new ResultReader(getResultSet(), handle.getJobId(), handle.getResultSetId());
        final ResultJobRecord.Status resultReaderStatus = resultReader.getStatus();
        if (resultReaderStatus == null) {
            LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        ResultStatus resultStatus = resultStatus(resultReaderStatus);
        Exception ex = extractException(resultReaderStatus);
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        final PrintWriter resultWriter = response.writer();
        response.setStatus(HttpResponseStatus.OK);
        SessionOutput sessionOutput = new SessionOutput(resultWriter);
        ResponsePrinter printer = new ResponsePrinter(sessionOutput);
        printer.begin();
        printer.addHeaderPrinter(new StatusPrinter(resultStatus));
        printer.printHeaders();
        if (ResultStatus.SUCCESS == resultStatus) {
            String servletPath = servletPath(request).replace("status", "result");
            String resHandle = "http://" + host(request) + servletPath + strHandle;
            printer.addResultPrinter(new ResultHandlePrinter(resHandle));
        } else if (ex != null) {
            printer.addResultPrinter(new ErrorsPrinter(Collections.singletonList(ExecutionError.of(ex))));
        }
        printer.printResults();
        printer.end();
        if (response.writer().checkError()) {
            LOGGER.warn("Error flushing output writer");
        }
    }

    ResultStatus resultStatus(ResultJobRecord.Status status) {
        switch (status.getState()) {
            case IDLE:
            case RUNNING:
                return ResultStatus.RUNNING;
            case SUCCESS:
                return ResultStatus.SUCCESS;
            case FAILED:
                return FAILED;
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
}
