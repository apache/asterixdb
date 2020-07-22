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

package org.apache.asterix.lang.common.parser;

import java.io.StringReader;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.entities.Function;

public class FunctionParser {

    private final IParserFactory parserFactory;

    public FunctionParser(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    public String getLanguage() {
        return parserFactory.getLanguage();
    }

    public FunctionDecl getFunctionDecl(Function function) throws CompilationException {
        if (!function.getLanguage().equals(getLanguage())) {
            throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE, getLanguage(),
                    function.getLanguage());
        }
        IParser parser = parserFactory.createParser(new StringReader(function.getFunctionBody()));
        try {
            return parser.parseFunctionBody(function.getSignature(), function.getParameterNames());
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_FUNCTION_DEFINITION, e, function.getSignature(),
                    e.getMessage());
        }
    }
}
