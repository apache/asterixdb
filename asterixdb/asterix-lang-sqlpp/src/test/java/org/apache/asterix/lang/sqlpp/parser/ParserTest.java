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
package org.apache.asterix.lang.sqlpp.parser;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.junit.Assert;
import org.junit.Test;

public class ParserTest {

    protected void assertParseError(String query, int expectedErrorLine) throws Exception {
        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        IParser parser = factory.createParser(query);
        try {
            parser.parse();
            throw new Exception("Expected error at line " + expectedErrorLine + " was not thrown");
        } catch (CompilationException e) {
            if (!e.getMessage().contains("Syntax error: In line " + expectedErrorLine)) {
                throw new Exception("Unexpected error", e);
            }
        }
    }

    protected void testLineEnding(String query, int expectedErrorLine) throws Exception {
        assertParseError(query, expectedErrorLine);
    }

    @Test
    public void testCR() throws Exception {
        testLineEnding("select\rvalue\r1", 3);
    }

    @Test
    public void testCRLF() throws Exception {
        testLineEnding("select\r\nvalue\r\n1", 3);
    }

    @Test
    public void testLF() throws Exception {
        testLineEnding("select\nvalue\n1", 3);
    }

    @Test
    public void testMultipleStatements() throws Exception {
        String query = "use x;\nselect 1;";
        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        IParser parser = factory.createParser(query);
        parser.parse();
        query = "use x;\n use x;;;;\n use x;\n select 1;";
        parser = factory.createParser(query);
        parser.parse();
    }

    @Test
    public void testUnseparatedStatements() throws Exception {
        String query = "create dataverse dv select 1";
        assertParseError(query, 1);
        query = "use x\n select 1;";
        assertParseError(query, 2);
        query = "use x;\n use x;\n use x\n select 1;";
        assertParseError(query, 4);
    }

    @Test
    public void testEmptyStatement() throws Exception {
        String query = "";
        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        IParser parser = factory.createParser(query);
        parser.parse();

        query = ";";
        parser = factory.createParser(query);
        parser.parse();

        query = ";;;";
        parser = factory.createParser(query);
        parser.parse();

        query = "\n\n";
        parser = factory.createParser(query);
        parser.parse();

        query = "; select 1;";
        parser = factory.createParser(query);
        parser.parse();

        query = ";;;;; select 1;;;";
        parser = factory.createParser(query);
        parser.parse();
    }

    @Test
    public void testStringLiterals() {
        // test different quote combinations
        String v1 = "a1";
        String[] prefixes = new String[] { "", "E" };
        String[] quotes = new String[] { "'", "\"" };

        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        StringBuilder qb = new StringBuilder();

        for (String p : prefixes) {
            for (String q1 : quotes) {
                for (String q2 : quotes) {
                    qb.setLength(0);
                    qb.append(p).append(q1).append(v1).append(q2).append(';');

                    boolean expectSuccess = q1.equals(q2);
                    String expectedValue = expectSuccess ? v1 : "Lexical error";
                    testParseStringLiteral(qb.toString(), expectSuccess, expectedValue, factory);
                }
            }
        }
    }

    @Test
    public void testStringLiteralsMulti() {
        // test different quote combinations
        String v1 = "a1", v2 = "b2", v3 = "c3";
        String[] prefixesStart = new String[] { "", "E" };
        String[] prefixesRest = new String[] { "", "E", " ", "\n" };
        String[] quotes = new String[] { "'", "\"" };

        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        StringBuilder qb = new StringBuilder();
        for (String p1 : prefixesStart) {
            for (String q1 : quotes) {
                for (String p2 : prefixesRest) {
                    for (String q2 : quotes) {
                        for (String p3 : prefixesRest) {
                            for (String q3 : quotes) {
                                qb.setLength(0);
                                qb.append(p1).append(q1).append(v1).append(q1);
                                qb.append(p2).append(q2).append(v2).append(q2);
                                qb.append(p3).append(q3).append(v3).append(q3);
                                qb.append(';');

                                boolean expectSuccess =
                                        p1.isEmpty() && p2.isEmpty() && p3.isEmpty() && q1.equals(q2) && q1.equals(q3);
                                String expectedValue =
                                        expectSuccess ? v1 + q1 + v2 + q1 + v3 : "Invalid string literal";
                                testParseStringLiteral(qb.toString(), expectSuccess, expectedValue, factory);
                            }
                        }
                    }
                }
            }
        }
    }

    private void testParseStringLiteral(String query, boolean expectSuccess, String expectedValueOrError,
            IParserFactory factory) {
        try {
            IParser parser = factory.createParser(query);
            Expression expr = parser.parseExpression();
            if (!expectSuccess) {
                Assert.fail("Unexpected parsing success for: " + query);
            }
            Assert.assertEquals(Expression.Kind.LITERAL_EXPRESSION, expr.getKind());
            LiteralExpr litExpr = (LiteralExpr) expr;
            Literal lit = litExpr.getValue();
            Assert.assertEquals(Literal.Type.STRING, lit.getLiteralType());
            String value = lit.getStringValue();
            Assert.assertEquals(expectedValueOrError, value);
        } catch (CompilationException e) {
            if (expectSuccess) {
                Assert.fail("Unexpected parsing failure for: " + query + " : " + e);
            } else {
                Assert.assertTrue(e.getMessage() + " does not contain: " + expectedValueOrError,
                        e.getMessage().contains(expectedValueOrError));
            }
        }
    }
}
