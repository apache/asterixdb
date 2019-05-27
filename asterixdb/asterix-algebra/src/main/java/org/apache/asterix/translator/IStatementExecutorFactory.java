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
package org.apache.asterix.translator;

import java.util.List;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;

/**
 * An interface for creating a statement executor which can be provided by
 * implementing {@code IStatementExecutorExtension}
 */
@FunctionalInterface
public interface IStatementExecutorFactory {

    /**
     * create a statement executor
     *
     * @param statements
     *            Statements to execute
     * @param output
     *            output and request configuration
     * @param compilationProvider
     *            provides query language related components
     * @param storageComponentProvider
     *            provides storage related components
     * @param responsePrinter
     * @return an implementation of {@code IStatementExecutor} thaxt is used to execute the passed list of statements
     */
    IStatementExecutor create(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, IStorageComponentProvider storageComponentProvider,
            IResponsePrinter responsePrinter);
}
