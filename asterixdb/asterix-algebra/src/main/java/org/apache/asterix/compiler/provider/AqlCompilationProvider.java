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
import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.aql.rewrites.AQLRewriterFactory;
import org.apache.asterix.lang.aql.visitor.AQLAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.translator.AqlExpressionToPlanTranslatorFactory;

public class AqlCompilationProvider implements ILangCompilationProvider {

    @Override
    public ILangExtension.Language getLanguage() {
        return ILangExtension.Language.AQL;
    }

    @Override
    public IParserFactory getParserFactory() {
        return new AQLParserFactory();
    }

    @Override
    public IRewriterFactory getRewriterFactory() {
        return new AQLRewriterFactory();
    }

    @Override
    public IAstPrintVisitorFactory getAstPrintVisitorFactory() {
        return new AQLAstPrintVisitorFactory();
    }

    @Override
    public ILangExpressionToPlanTranslatorFactory getExpressionToPlanTranslatorFactory() {
        return new AqlExpressionToPlanTranslatorFactory();
    }

    @Override
    public IRuleSetFactory getRuleSetFactory() {
        return new DefaultRuleSetFactory();
    }
}
