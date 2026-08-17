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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public interface IParser {

    List<Statement> parse() throws CompilationException;

    Expression parseExpression() throws CompilationException;

    List<String> parseMultipartIdentifier() throws CompilationException;

    FunctionDecl parseFunctionBody(FunctionSignature signature, List<String> paramNames, boolean isStored)
            throws CompilationException;

    ViewDecl parseViewBody(DatasetFullyQualifiedName viewName) throws CompilationException;

    /**
     * Gets the warnings generated during parsing
     */
    default void getWarnings(IWarningCollector outWarningCollector) {
    }

    /**
     * Gets the warnings generated during parsing up to the max number argument.
     */
    default void getWarnings(Collection<? super Warning> outWarnings, long maxWarnings) {
    }

    /**
     * Gets the count of all warnings generated during parsing.
     */
    default long getTotalWarningsCount() {
        return 0L;
    }

    /**
     * The warnings raised while parsing each statement, in parse order. A parser that does not attribute its warnings
     * to a statement returns none, and they are reported for the request as a whole.
     */
    default List<Set<Warning>> getWarningsPerStatement() {
        return Collections.emptyList();
    }

    /**
     * The text of each statement of the request, in parse order: from a statement's first token to its last, without
     * the semicolon that ends it. A parser that does not keep the text of a statement returns none.
     */
    default List<String> getStatementTexts() {
        return Collections.emptyList();
    }
}
