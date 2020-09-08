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

package org.apache.asterix.test.sqlpp;

import java.util.List;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DataverseNameTest;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.junit.Assert;

public class DataverseNameParserTest extends DataverseNameTest {

    private final IParserFactory parserFactory = new SqlppParserFactory();

    private final IRewriterFactory rewriterFactory = new SqlppRewriterFactory(parserFactory);

    @Override
    protected void testDataverseNameImpl(DataverseName dataverseName, List<String> parts, String expectedCanonicalForm,
            String expectedDisplayForm) throws Exception {
        super.testDataverseNameImpl(dataverseName, parts, expectedCanonicalForm, expectedDisplayForm);

        String displayForm = dataverseName.toString();

        // check parse-ability of the display form
        IParser parser = parserFactory.createParser(displayForm);
        Expression expr = parser.parseExpression();
        IStatementRewriter rewriter = rewriterFactory.createStatementRewriter();

        for (int i = parts.size() - 1; i >= 0; i--) {
            String part = parts.get(i);
            String parsedPart;
            if (i > 0) {
                Assert.assertEquals(Expression.Kind.FIELD_ACCESSOR_EXPRESSION, expr.getKind());
                FieldAccessor faExpr = (FieldAccessor) expr;
                parsedPart = faExpr.getIdent().getValue();
                expr = faExpr.getExpr();
            } else {
                Assert.assertEquals(Expression.Kind.VARIABLE_EXPRESSION, expr.getKind());
                VariableExpr varExpr = (VariableExpr) expr;
                parsedPart = rewriter.toFunctionParameterName(varExpr.getVar());
            }
            Assert.assertEquals("unexpected parsed part at position " + i + " in " + parts, part, parsedPart);
        }
    }
}
