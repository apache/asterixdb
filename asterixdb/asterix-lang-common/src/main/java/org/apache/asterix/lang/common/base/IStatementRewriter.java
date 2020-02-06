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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;

public interface IStatementRewriter {

    /**
     * Returns {@code true} if given statement kind is handled by this rewriter.
     */
    boolean isRewritable(Statement.Kind kind);

    /**
     * @param statement,
     *            a non-query statement.
     * @param metadataProvider
     *            a metadata provider
     */
    void rewrite(Statement statement, MetadataProvider metadataProvider) throws CompilationException;

    String toExternalVariableName(String statementParameterName);

    String toFunctionParameterName(VarIdentifier paramVar);
}
