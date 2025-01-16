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

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.translator.SessionOutput;

public class UnstableStatementExecutorFactory extends DefaultStatementExecutorFactory {
    public UnstableStatementExecutorFactory(ExecutorService executorService) {
        super(executorService);
    }

    @Override
    public QueryTranslator create(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, IStorageComponentProvider storageComponentProvider,
            IResponsePrinter responsePrinter) {
        return new RetryingQueryTranslator(appCtx, statements, output, compilationProvider, executorService,
                responsePrinter);
    }
}
