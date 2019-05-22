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
package org.apache.asterix.algebra.base;

import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.api.result.IResultMetadata;

/**
 * The interface is in charge of translating language expressions into logical query plans.
 */
public interface ILangExpressionToPlanTranslator {

    /**
     * Translate a query.
     *
     * @param query,
     *            the AST of a query.
     * @param outputDatasetName,
     *            the output dataset name (only for insert/delete).
     * @param stmt,
     *            the compiled dml statement (only for insert/delete).
     * @param resultMetadata,
     *            some result metadata that can be retrieved with the result
     * @return a logical query plan for the query.
     * @throws AlgebricksException
     */
    public ILogicalPlan translate(Query query, String outputDatasetName, ICompiledDmlStatement stmt,
            IResultMetadata resultMetadata) throws AlgebricksException;

    /**
     * Translates a load statement.
     *
     * @param stmt,
     *            the compiled load statement.
     * @return a logical query plan for the load statement.
     * @throws AlgebricksException
     */
    public ILogicalPlan translateLoad(ICompiledDmlStatement stmt) throws AlgebricksException;

    /**
     * @return the current minimum available variable id.
     */
    public int getVarCounter();
}
