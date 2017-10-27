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
import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_DATASET_ATTR;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionConfig.OutputFormat;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class RestApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(RestApiServlet.class.getName());
    private final ICcApplicationContext appCtx;
    private final ILangCompilationProvider compilationProvider;
    private final IParserFactory parserFactory;
    private final IStatementExecutorFactory statementExecutorFactory;
    private final IStorageComponentProvider componentProvider;

    public RestApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, IStatementExecutorFactory statementExecutorFactory,
            IStorageComponentProvider componentProvider) {
        super(ctx, paths);
        this.appCtx = appCtx;
        this.compilationProvider = compilationProvider;
        this.parserFactory = compilationProvider.getParserFactory();
        this.statementExecutorFactory = statementExecutorFactory;
        this.componentProvider = componentProvider;
    }

    /**
     * Initialize the Content-Type of the response, and construct a
     * SessionConfig with the appropriate output writer and output-format
     * based on the Accept: header and other servlet parameters.
     */
    static SessionOutput initResponse(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_PLAIN, HttpUtil.Encoding.UTF8);
        // CLEAN_JSON output is the default; most generally useful for a
        // programmatic HTTP API
        OutputFormat format = OutputFormat.CLEAN_JSON;
        // First check the "output" servlet parameter.
        String output = request.getParameter("output");
        String accept = request.getHeader("Accept", "");
        if (output != null) {
            if ("CSV".equals(output)) {
                format = OutputFormat.CSV;
            } else if ("ADM".equals(output)) {
                format = OutputFormat.ADM;
            }
        } else {
            // Second check the Accept: HTTP header.
            if (accept.contains("application/x-adm")) {
                format = OutputFormat.ADM;
            } else if (accept.contains("text/csv")) {
                format = OutputFormat.CSV;
            }
        }
        PlanFormat planFormat =
                PlanFormat.get(request.getParameter("plan-format"), "plan format", PlanFormat.STRING, LOGGER);

        // If it's JSON, check for the "lossless" flag

        if (format == OutputFormat.CLEAN_JSON
                && ("true".equals(request.getParameter("lossless")) || accept.contains("lossless=true"))) {
            format = OutputFormat.LOSSLESS_JSON;
        }

        SessionOutput.ResultAppender appendHandle = (app, handle) -> app.append("{ \"").append("handle")
                .append("\":" + " \"").append(handle).append("\" }");
        SessionConfig sessionConfig = new SessionConfig(format, planFormat);

        // If it's JSON or ADM, check for the "wrapper-array" flag. Default is
        // "true" for JSON and "false" for ADM. (Not applicable for CSV.)
        boolean wrapperArray = format == OutputFormat.CLEAN_JSON || format == OutputFormat.LOSSLESS_JSON;
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
                HttpUtil.setContentType(response, "application/x-adm");
                break;
            case CLEAN_JSON:
                // No need to reflect "clean-ness" in output type; fall through
            case LOSSLESS_JSON:
                HttpUtil.setContentType(response, "application/json");
                break;
            case CSV:
                // Check for header parameter or in Accept:.
                if ("present".equals(request.getParameter("header")) || accept.contains("header=present")) {
                    HttpUtil.setContentType(response, "text/csv; header=present");
                    sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, true);
                } else {
                    HttpUtil.setContentType(response, "text/csv; header=absent");
                }
                break;
            default:
                throw new IOException("Unknown format " + format);
        }
        return new SessionOutput(sessionConfig, response.writer(), null, null, appendHandle, null);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        getOrPost(request, response);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        getOrPost(request, response);
    }

    private void getOrPost(IServletRequest request, IServletResponse response) {
        try {
            String query = query(request);
            // enable cross-origin resource sharing
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
            SessionOutput sessionOutput = initResponse(request, response);
            QueryTranslator.ResultDelivery resultDelivery = whichResultDelivery(request);
            doHandle(response, query, sessionOutput, resultDelivery);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.log(Level.WARNING, "Failure handling request", e);
            return;
        }
    }

    private void doHandle(IServletResponse response, String query, SessionOutput sessionOutput,
            ResultDelivery resultDelivery) throws JsonProcessingException {
        try {
            response.setStatus(HttpResponseStatus.OK);
            IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
            IHyracksDataset hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
            if (hds == null) {
                synchronized (ctx) {
                    hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
                    if (hds == null) {
                        hds = new HyracksDataset(hcc, appCtx.getCompilerProperties().getFrameSize(),
                                ResultReader.NUM_READERS);
                        ctx.put(HYRACKS_DATASET_ATTR, hds);
                    }
                }
            }
            IParser parser = parserFactory.createParser(query);
            List<Statement> aqlStatements = parser.parse();
            validate(aqlStatements);
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator = statementExecutorFactory.create(appCtx, aqlStatements, sessionOutput,
                    compilationProvider, componentProvider);
            final IRequestParameters requestParameters =
                    new RequestParameters(hds, resultDelivery, new IStatementExecutor.Stats(), null, null, null);
            translator.compileAndExecute(hcc, null, requestParameters);
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            String errorMessage = ResultUtil.buildParseExceptionMessage(pe, query);
            ObjectNode errorResp =
                    ResultUtil.getErrorResponse(2, errorMessage, "", ResultUtil.extractFullStackTrace(pe));
            sessionOutput.out().write(OBJECT_MAPPER.writeValueAsString(errorResp));
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            ResultUtil.apiErrorHandler(sessionOutput.out(), e);
        }
    }

    //TODO: Both Get and Post of this API must use the same parameter names
    private String query(IServletRequest request) {
        if (request.getHttpRequest().method() == HttpMethod.POST) {
            return HttpUtil.getRequestBody(request);
        } else {
            return getQueryParameter(request);
        }
    }

    private void validate(List<Statement> aqlStatements) throws AsterixException {
        for (Statement st : aqlStatements) {
            if ((st.getCategory() & getAllowedCategories()) == 0) {
                throw new AsterixException(String.format(getErrorMessage(), st.getKind()));
            }
        }
    }

    protected QueryTranslator.ResultDelivery whichResultDelivery(IServletRequest request) {
        String mode = request.getParameter("mode");
        if (mode != null) {
            if ("asynchronous".equals(mode) || "async".equals(mode)) {
                return QueryTranslator.ResultDelivery.ASYNC;
            } else if ("asynchronous-deferred".equals(mode) || "deferred".equals(mode)) {
                return QueryTranslator.ResultDelivery.DEFERRED;
            }
        }
        return QueryTranslator.ResultDelivery.IMMEDIATE;
    }

    protected abstract String getQueryParameter(IServletRequest request);

    protected abstract byte getAllowedCategories();

    protected abstract String getErrorMessage();
}
