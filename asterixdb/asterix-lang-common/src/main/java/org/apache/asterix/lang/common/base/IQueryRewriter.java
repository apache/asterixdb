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
package org.apache.asterix.lang.common.base;

import java.util.Collection;
import java.util.Set;

import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;

public interface IQueryRewriter {

    /**
     * Rewrite a query at the AST level.
     * @param topExpr,
     *          the query to be rewritten.
     * @param context
     *          rewriting context
     * @param allowNonStoredUdfCalls
     *          whether calls to non-stored user-defined functions should be resolved
     * @param inlineUdfsAndViews
     *          whether user defined functions should be inlines
     * @param externalVars
     */
    void rewrite(LangRewritingContext context, IReturningStatement topExpr, boolean allowNonStoredUdfCalls,
            boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars) throws CompilationException;

    /**
     * Find the function calls used by a given expression
     */
    void getFunctionCalls(Expression expression, Collection<? super AbstractCallExpression> outCalls)
            throws CompilationException;

    /**
     * Find all external variables (positional and named variables) in given expression
     */
    Set<VariableExpr> getExternalVariables(Expression expr) throws CompilationException;

    VarIdentifier toExternalVariableName(String statementParameterName);

    String toFunctionParameterName(VarIdentifier paramVar);

    Query createFunctionAccessorQuery(FunctionDecl functionDecl);

    Query createViewAccessorQuery(ViewDecl viewDecl, INamespaceResolver namespaceResolver);
}
