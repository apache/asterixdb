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
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.hyracks.api.exceptions.Warning;

public interface IParser {

    List<Statement> parse() throws CompilationException;

    Expression parseExpression() throws CompilationException;

    List<String> parseMultipartIdentifier() throws CompilationException;

    FunctionDecl parseFunctionBody(FunctionSignature signature, List<String> paramNames) throws CompilationException;

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
}
