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
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.IHyracksDataset;
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

        IHyracksDataset hds = getHyracksDataset();
        ResultReader resultReader = new ResultReader(hds, handle.getJobId(), handle.getResultSetId());

        final DatasetJobRecord.Status resultReaderStatus = resultReader.getStatus();
        if (resultReaderStatus == null) {
            LOGGER.log(Level.INFO, "No results for: \"" + strHandle + "\"");
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }

        ResultStatus resultStatus = resultStatus(resultReaderStatus);
        Exception ex = extractException(resultReaderStatus);

        final StringWriter stringWriter = new StringWriter();
        final PrintWriter resultWriter = new PrintWriter(stringWriter);

        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        HttpResponseStatus httpStatus = HttpResponseStatus.OK;

        resultWriter.print("{\n");
        ResultUtil.printStatus(resultWriter, resultStatus, (ex != null) || ResultStatus.SUCCESS == resultStatus);

        if (ResultStatus.SUCCESS == resultStatus) {
            String servletPath = servletPath(request).replace("status", "result");
            String resHandle = "http://" + host(request) + servletPath + strHandle;
            printHandle(resultWriter, resHandle, false);
        } else if (ex != null) {
            ResultUtil.printError(resultWriter, ex, false);
        }

        resultWriter.print("}\n");
        resultWriter.flush();
        String result = stringWriter.toString();

        response.setStatus(httpStatus);
        response.writer().print(result);
        if (response.writer().checkError()) {
            LOGGER.warn("Error flushing output writer");
        }
    }

    ResultStatus resultStatus(DatasetJobRecord.Status status) {
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

    Exception extractException(DatasetJobRecord.Status status) {
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
