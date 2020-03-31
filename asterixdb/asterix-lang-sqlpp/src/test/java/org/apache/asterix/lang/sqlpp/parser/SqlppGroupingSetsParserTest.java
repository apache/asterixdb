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

import static org.apache.asterix.lang.sqlpp.parser.SqlppGroupingSetsParser.GROUPING_SETS_LIMIT;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.SqlppFormatPrintVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.util.MathUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test rewriting from GROUP BY's grouping elements into grouping sets:
 * <p>
 * GROUP BY {0} => GROUP BY GROUPING SETS ({1})
 */
@RunWith(Parameterized.class)
public class SqlppGroupingSetsParserTest {

    private static final int GROUPING_SETS_LIMIT_LOG2 = MathUtil.log2Floor(GROUPING_SETS_LIMIT);

    private static final int GROUPING_SETS_LIMIT_SQRT = (int) Math.ceil(Math.sqrt(GROUPING_SETS_LIMIT));

    private static final String ERR_PREFIX = "ASX";

    private static final String ERR_SYNTAX = ERR_PREFIX + ErrorCode.PARSE_ERROR;

    private static final String ERR_OVERFLOW = ERR_PREFIX + ErrorCode.COMPILATION_GROUPING_SETS_OVERFLOW;

    private static final String ERR_ALIAS = ERR_PREFIX + ErrorCode.COMPILATION_UNEXPECTED_ALIAS;

    @Parameterized.Parameters(name = "{index}: GROUP BY {0}")
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][] {
                        // GROUP BY {0} => GROUP BY GROUPING SETS ({1})
                        //
                        // 1. Basic
                        //
                        { "()", "()" },
                        //
                        { "(), ()", "()" },
                        //
                        { "a", "(a)" },
                        //
                        { "a,b", "(a,b)" },
                        //
                        { "a,(b,c)", "(a,b,c)" },
                        //
                        { "(a,b),(c,d)", "(a,b,c,d)" },
                        //
                        // 2. Rollup
                        //
                        { "rollup(a,b,c)", "(a,b,c)(a,b)(a)()" },
                        //
                        { "Rollup(a,(b,c),d)", "(a,b,c,d)(a,b,c)(a)()" },
                        //
                        { "RollUP((a,b),(c,d))", "(a,b,c,d)(a,b)()" },
                        //
                        { "a,ROLLUP(a,b)", "(a,b)(a)(a)" },
                        //
                        { "a,ROLLUP(a,b),c", "(a,b,c)(a,c)(a,c)" },
                        //
                        { "a,b,ROLLUP(c,d)", "(a,b,c,d)(a,b,c)(a,b)" },
                        //
                        { "ROLLUP(a,b),ROLLUP(c,d)", "(a,b,c,d)(a,b,c)(a,b)(a,c,d)(a,c)(a)(c,d)(c)()" },
                        //
                        // 3. Cube
                        //
                        { "cube(a,b,c)", "(a,b,c)(a,b)(a,c)(a)(b,c)(b)(c)()" },
                        //
                        { "Cube(a,(b,c),d)", "(a,b,c,d)(a,b,c)(a,d)(a)(b,c,d)(b,c)(d)()" },
                        //
                        { "CubE((a,b),(c,d))", "(a,b,c,d)(a,b)(c,d)()" },
                        //
                        { "a,CUBE(a,b)", "(a,b)(a)(a,b)(a)" },
                        //
                        { "a,CUBE(a,b),c", "(a,b,c)(a,c)(a,b,c)(a,c)" },
                        //
                        { "a,b,CUBE(c,d)", "(a,b,c,d)(a,b,c)(a,b,d)(a,b)" },
                        //
                        { "CUBE(a,b),CUBE(c,d)",
                                "(a,b,c,d)(a,b,c)(a,b,d)(a,b)(a,c,d)(a,c)(a,d)(a)(b,c,d)(b,c)(b,d)(b)(c,d)(c)(d)()" },
                        //
                        // 4. Rollup + Cube
                        //
                        { "ROLLUP(a,b),CUBE(c,d)", "(a,b,c,d)(a,b,c)(a,b,d)(a,b)(a,c,d)(a,c)(a,d)(a)(c,d)(c)(d)()" },
                        //
                        { "CUBE(a,b),ROLLUP(c,d)", "(a,b,c,d)(a,b,c)(a,b)(a,c,d)(a,c)(a)(b,c,d)(b,c)(b)(c,d)(c)()" },
                        //
                        // 5. Grouping Sets
                        //
                        { "grouping sets(())", "()" },
                        //
                        { "Grouping Sets((), ())", "()()" },
                        //
                        { "Grouping setS(()), GROUPING SETS(())", "()" },
                        //
                        { "GROUPING SETS((a),(a,b))", "(a)(a,b)" },
                        //
                        { "GROUPING SETS((a,b),(a,b))", "(a,b)(a,b)" },
                        //
                        { "GROUPING SETS((a,b)),GROUPING SETS((a,b))", "(a,b)" },
                        //
                        { "GROUPING SETS((a,b),(c)),GROUPING SETS((d,e),())", "(a,b,d,e)(a,b)(c,d,e)(c)" },
                        //
                        { "GROUPING SETS(ROLLUP(a,b),ROLLUP(c,d))", "(a,b)(a)()(c,d)(c)()" },
                        //
                        { "GROUPING SETS(ROLLUP(a,b)), GROUPING SETS(ROLLUP(c,d))",
                                "(a,b,c,d)(a,b,c)(a,b)(a,c,d)(a,c)(a)(c,d)(c)()" },
                        //
                        { "GROUPING SETS((a, b), GROUPING SETS((c,d), (e,f)))", "(a,b)(c,d)(e,f)" },
                        //
                        { "GROUPING SETS(ROLLUP(a,b)),GROUPING SETS(ROLLUP(c,d))",
                                "(a,b,c,d)(a,b,c)(a,b)(a,c,d)(a,c)(a)(c,d)(c)()" },
                        //
                        // 6. Variable names (aliases)
                        //
                        { "a as x, b as y", "(a as x,b as y)" },
                        //
                        { "ROLLUP(a as x, b as y),ROLLUP(a, b)",
                                "(a as x,b as y)(a as x,b as y)(a as x,b as y)(a as x,b as y)"
                                        + "(a as x)(a as x)(a as x,b as y)(a as x)()" },
                        //
                        { "CUBE(a as x, b as y),CUBE(a, b)",
                                "(a as x,b as y)(a as x,b as y)(a as x,b as y)(a as x,b as y)"
                                        + "(a as x,b as y)(a as x)(a as x,b as y)(a as x)(b as y,a as x)(b as y,a as x)(b as y)(b as y)"
                                        + "(a as x,b as y)(a as x)(b as y)()" },
                        //
                        { "GROUPING SETS((e1 as x, e2 as y)), GROUPING SETS((e1, e2))", "(e1 as x,e2 as y)" },
                        //
                        { "GROUPING SETS((e1 as x, e2 as y), (e1, e2))", "(e1 as x,e2 as y)(e1 as x,e2 as y)" },
                        //
                        // 7. Errors
                        //
                        // Syntax error
                        { "ROLLUP()", ERR_SYNTAX },
                        //
                        // Syntax error
                        { "CUBE()", ERR_SYNTAX },
                        //
                        // Too many grouping sets when expanding a rollup
                        { String.format("ROLLUP(%s)", generateSimpleGroupingSet("a", GROUPING_SETS_LIMIT)),
                                ERR_OVERFLOW },
                        //
                        // Too many grouping sets when expanding a cube
                        { String.format("CUBE(%s)", generateSimpleGroupingSet("a", GROUPING_SETS_LIMIT_LOG2 + 1)),
                                ERR_OVERFLOW },
                        //
                        // Too many grouping sets when doing a cross product of grouping sets
                        { String.format("GROUPING SETS(%s), GROUPING SETS(%s)",
                                generateSimpleGroupingSet("a", GROUPING_SETS_LIMIT_SQRT),
                                generateSimpleGroupingSet("b", GROUPING_SETS_LIMIT_SQRT)), ERR_OVERFLOW },
                        //
                        // Unexpected aliases
                        //
                        { "ROLLUP(a as x, b),ROLLUP(a, b as y)", ERR_ALIAS },
                        //
                        { "CUBE(a as x, b),CUBE(a, b as y)", ERR_ALIAS },
                        //
                        { "GROUPING SETS((e1 as x, e2), (e1, e2 as y))", ERR_ALIAS },
                        //
                        { "GROUPING SETS((e1 as x, e2)), GROUPING SETS((e1, e2 as y))", ERR_ALIAS },
                        //
                        { "GROUPING SETS((e1 as a, e2 as b)), GROUPING SETS((e1 as c, e2 as d))", ERR_ALIAS }, });
    }

    private final String groupbyInput;

    private final String expectedGroupingSets;

    private final String expectedErrorCode;

    public SqlppGroupingSetsParserTest(String groupbyInput, String expectedResult) {
        this.groupbyInput = groupbyInput;
        if (expectedResult.startsWith(ERR_PREFIX)) {
            this.expectedGroupingSets = null;
            this.expectedErrorCode = expectedResult;
        } else {
            this.expectedGroupingSets = expectedResult;
            this.expectedErrorCode = null;
        }
    }

    @Test
    public void test() throws Exception {
        SqlppParserFactory parserFactory = new SqlppParserFactory();
        String groupbyClause = "GROUP BY " + groupbyInput;
        String query = "SELECT COUNT(*) FROM test " + groupbyClause + ";";
        // parse 2 queries so we can test calling rewrite() multiple times on the same instance
        IParser parser = parserFactory.createParser(query + query);
        List<Statement> statements;
        try {
            statements = parser.parse();
        } catch (CompilationException e) {
            if (expectedErrorCode == null) {
                throw e;
            } else if (e.getMessage().contains(expectedErrorCode)) {
                return; // Found expected error code. SUCCESS
            } else {
                Assert.fail(String.format("Unable to find expected error code %s in error message: %s",
                        expectedErrorCode, e.getMessage()));
                throw new IllegalStateException();
            }
        }
        Assert.assertEquals(2, statements.size());

        for (Statement statement : statements) {
            String groupingSets = extractGroupingSets(statement);
            Assert.assertEquals(expectedGroupingSets, groupingSets);
        }
    }

    private String extractGroupingSets(Statement stmt) throws Exception {
        SqlppGroupingSetsRewriterTestVisitor visitor = new SqlppGroupingSetsRewriterTestVisitor();
        stmt.accept(visitor, null);
        return visitor.printGroupingSets();
    }

    private static class SqlppGroupingSetsRewriterTestVisitor extends AbstractSqlppSimpleExpressionVisitor {

        private final List<List<GbyVariableExpressionPair>> groupingSets = new ArrayList<>();

        private final StringWriter stringWriter = new StringWriter();

        private final PrintWriter printWriter = new PrintWriter(stringWriter);

        private final SqlppFormatPrintVisitor printVisitor = new SqlppFormatPrintVisitor(printWriter);

        @Override
        public Expression visit(GroupbyClause gc, ILangExpression arg) throws CompilationException {
            groupingSets.addAll(gc.getGbyPairList());
            return super.visit(gc, arg);
        }

        public String printGroupingSets() throws CompilationException {
            StringBuffer sb = stringWriter.getBuffer();
            sb.delete(0, sb.length());

            for (List<GbyVariableExpressionPair> groupingSet : groupingSets) {
                printWriter.append('(');
                String sep = "";
                for (GbyVariableExpressionPair pair : groupingSet) {
                    printWriter.append(sep);
                    sep = ",";
                    pair.getExpr().accept(printVisitor, 0);
                    if (pair.getVar() != null) {
                        String ident = SqlppVariableUtil.toUserDefinedName(pair.getVar().getVar().getValue());
                        printWriter.append(" as ").append(ident);
                    }
                }
                printWriter.append(')');
            }
            printWriter.flush();
            return stringWriter.toString();
        }
    }

    private static String generateSimpleGroupingSet(String prefix, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(prefix).append(i);
        }
        return sb.toString();
    }
}
