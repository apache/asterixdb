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

package org.apache.asterix.test.cloud_storage;

import static org.apache.asterix.cloud.clients.UnstableCloudClient.ERROR_RATE;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.TruncateDatasetStatement;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RetryingQueryTranslator extends QueryTranslator {

    private static final Logger LOGGER = LogManager.getLogger();

    public RetryingQueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, ExecutorService executorService,
            IResponsePrinter responsePrinter) {
        super(appCtx, statements, output, compilationProvider, executorService, responsePrinter);
    }

    @Override
    public void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters, Creator creator) throws Exception {
        int times = 100;
        Exception ex = null;
        double initialErrorRate = ERROR_RATE.get();
        try {
            while (times-- > 0) {
                try {
                    super.handleCreateIndexStatement(metadataProvider, stmt, hcc, requestParameters, creator);
                    ex = null;
                    break;
                } catch (Exception e) {
                    ERROR_RATE.set(Double.max(ERROR_RATE.get() - 0.01d, 0.01d));
                    if (retryOnFailure(e)) {
                        LOGGER.error("Attempt: {}, Failed to create index", 100 - times, e);
                        metadataProvider.getLocks().reset();
                        ex = e;
                    } else {
                        throw e;
                    }
                }
            }
            if (ex != null) {
                throw ex;
            }
        } finally {
            ERROR_RATE.set(initialErrorRate);
        }
    }

    @Override
    public void handleAnalyzeStatement(MetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        int times = 100;
        Exception ex = null;
        while (times-- > 0) {
            try {
                super.handleAnalyzeStatement(metadataProvider, stmt, hcc, requestParameters);
                ex = null;
                break;
            } catch (Exception e) {
                if (retryOnFailure(e)) {
                    LOGGER.error("Attempt: {}, Failed to create index", 100 - times, e);
                    metadataProvider.getLocks().reset();
                    ex = e;
                } else {
                    throw e;
                }
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public void handleLoadStatement(MetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        int times = 100;
        Exception ex = null;
        double initialErrorRate = ERROR_RATE.get();
        try {
            while (times-- > 0) {
                try {
                    super.handleLoadStatement(metadataProvider, stmt, hcc, requestParameters);
                    ex = null;
                    break;
                } catch (Exception e) {
                    ERROR_RATE.set(Double.max(ERROR_RATE.get() - 0.01d, 0.01d));
                    if (retryOnFailure(e)) {
                        LOGGER.error("Attempt: {}, Failed to load", 100 - times, e);
                        metadataProvider.getLocks().reset();
                        ex = e;
                        LoadStatement loadStmt = (LoadStatement) stmt;
                        TruncateDatasetStatement truncateDatasetStatement =
                                new TruncateDatasetStatement(loadStmt.getNamespace(), loadStmt.getDatasetName(), true);
                        super.handleDatasetTruncateStatement(metadataProvider, truncateDatasetStatement,
                                requestParameters);
                        metadataProvider.getLocks().reset();

                    } else {
                        throw e;
                    }
                }
            }
            if (ex != null) {
                throw ex;
            }
        } finally {
            ERROR_RATE.set(initialErrorRate);
        }
    }

    private boolean retryOnFailure(Exception e) {
        if (e instanceof HyracksDataException) {
            return ((HyracksDataException) e).getErrorCode() == ErrorCode.FAILED_IO_OPERATION.intValue()
                    && ExceptionUtils.getRootCause(e).getMessage().contains("Simulated error");
        }
        return false;
    }
}
