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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;

public interface IQueryRewriter {

    /**
     * Rewrite a query at the AST level.
     *
     * @param declaredFunctions,
     *            a list of declared functions associated with the query.
     * @param topExpr,
     *            the query to be rewritten.
     * @param metadataProvider,
     *            providing the definition of created (i.e., stored) user-defined functions.
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     */
    public void rewrite(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider,
            LangRewritingContext context) throws AsterixException;
}
