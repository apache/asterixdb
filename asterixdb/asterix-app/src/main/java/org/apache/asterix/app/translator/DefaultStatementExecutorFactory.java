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
package org.apache.asterix.app.translator;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.control.common.utils.HyracksThreadFactory;

public class DefaultStatementExecutorFactory implements IStatementExecutorFactory {

    protected final ExecutorService executorService;

    /*
     * @deprecated use other constructor
     */
    public DefaultStatementExecutorFactory() {
        this(Executors.newSingleThreadExecutor(
                new HyracksThreadFactory(DefaultStatementExecutorFactory.class.getSimpleName())));
    }

    public DefaultStatementExecutorFactory(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public IStatementExecutor create(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, IStorageComponentProvider storageComponentProvider,
            IResponsePrinter responsePrinter) {
        return new QueryTranslator(appCtx, statements, output, compilationProvider, executorService, responsePrinter);
    }
}
