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
package org.apache.asterix.compiler.provider;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.lang.common.base.IAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;

public interface ILangCompilationProvider {

    /**
     * @return the parser factory of a language implementation.
     */
    public IParserFactory getParserFactory();

    /**
     * @return the rewriter factory of a language implementation.
     */
    public IRewriterFactory getRewriterFactory();

    /**
     * @return the AST printer factory of a language implementation.
     */
    public IAstPrintVisitorFactory getAstPrintVisitorFactory();

    /**
     * @return the language expression to logical query plan translator factory of a language implementation.
     */
    public ILangExpressionToPlanTranslatorFactory getExpressionToPlanTranslatorFactory();

}
