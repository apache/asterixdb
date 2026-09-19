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

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.TimeTravelSpec;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;
import org.junit.Assert;
import org.junit.Test;

/**
 * Grammar coverage for the {@code AT SNAPSHOT} / {@code AT TIMESTAMP} value, which accepts an arbitrary
 * expression rather than only a string literal. Whether the expression can actually be folded to a constant is
 * decided later, during expression-to-plan translation, so this only pins down what parses and into what.
 */
public class TimeTravelParserTest {

    @Test
    public void testStringLiteralSnapshot() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT \"8574821\";");
        Assert.assertEquals(TimeTravel.Type.SNAPSHOT_ID, spec.getType());
        Assert.assertEquals("8574821", stringLiteral(spec));
    }

    /**
     * A bare integer used to be a parse error, which made the natural spelling of a snapshot id unusable.
     */
    @Test
    public void testIntegerLiteralSnapshot() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT 8574821;");
        Assert.assertEquals(TimeTravel.Type.SNAPSHOT_ID, spec.getType());
        Assert.assertEquals(Expression.Kind.LITERAL_EXPRESSION, spec.getValueExpression().getKind());
        Assert.assertEquals(Literal.Type.LONG, ((LiteralExpr) spec.getValueExpression()).getValue().getLiteralType());
    }

    @Test
    public void testTimestampKeyword() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT TIMESTAMP \"2026-08-20\";");
        Assert.assertEquals(TimeTravel.Type.SNAPSHOT_TIMESTAMP, spec.getType());
        Assert.assertEquals("2026-08-20", stringLiteral(spec));
    }

    @Test
    public void testNamedParameter() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT TIMESTAMP $ts;");
        Assert.assertEquals(Expression.Kind.VARIABLE_EXPRESSION, spec.getValueExpression().getKind());
    }

    @Test
    public void testPositionalParameter() throws Exception {
        Assert.assertEquals(Expression.Kind.VARIABLE_EXPRESSION,
                fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT $1;").getValueExpression().getKind());
        Assert.assertEquals(Expression.Kind.VARIABLE_EXPRESSION,
                fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT ?;").getValueExpression().getKind());
    }

    @Test
    public void testDateArithmetic() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT TIMESTAMP current_date() - duration(\"P1D\");");
        Assert.assertEquals(TimeTravel.Type.SNAPSHOT_TIMESTAMP, spec.getType());
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, spec.getValueExpression().getKind());
    }

    @Test
    public void testFunctionCall() throws Exception {
        TimeTravelSpec spec =
                fromTermTimeTravel("SELECT * FROM c AT TIMESTAMP unix_time_from_date_in_ms(date(\"2026-08-20\"));");
        Assert.assertEquals(Expression.Kind.CALL_EXPRESSION, spec.getValueExpression().getKind());
    }

    /**
     * {@code AT} is overloaded: it introduces both the time travel value and the positional variable, in either
     * order.
     */
    @Test
    public void testTimeTravelThenPositionalVariable() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT SNAPSHOT 1 AT p;");
        Assert.assertTrue(fromTerm.hasTimeTravel());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
    }

    @Test
    public void testPositionalVariableThenTimeTravel() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT p AT SNAPSHOT 1;");
        Assert.assertTrue(fromTerm.hasTimeTravel());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
    }

    @Test
    public void testPositionalVariableOnly() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT p;");
        Assert.assertFalse(fromTerm.hasTimeTravel());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
    }

    /**
     * The value is a full expression now, so the parser has to stop at the {@code AT} that introduces the
     * positional variable instead of trying to continue the expression. The bare-literal cases above cannot
     * detect that, because a literal ends where an expression could not have continued anyway.
     */
    @Test
    public void testTimeTravelExpressionThenPositionalVariable() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT SNAPSHOT 5 - 4 AT p;");
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, fromTerm.getTimeTravel().getValueExpression().getKind());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
    }

    @Test
    public void testPositionalVariableThenTimeTravelExpression() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT p AT SNAPSHOT 5 - 4;");
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, fromTerm.getTimeTravel().getValueExpression().getKind());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
    }

    /**
     * Alias, value and positional variable together: the alias binds before either {@code AT}, so all three
     * have to come out distinct.
     */
    @Test
    public void testAliasTimeTravelAndPositionalVariable() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AS x AT SNAPSHOT 5 - 4 AT p;");
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, fromTerm.getTimeTravel().getValueExpression().getKind());
        Assert.assertTrue(fromTerm.hasPositionalVariable());
        Assert.assertNotEquals(fromTerm.getLeftVariable().getVar(), fromTerm.getPositionalVariable().getVar());
    }

    /**
     * The JOIN clause repeats the same overloaded {@code AT} structure, from a separate grammar production.
     */
    @Test
    public void testJoinTimeTravelAndPositionalVariable() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c JOIN d AS y AT SNAPSHOT 5 - 4 AT q ON c.id = d.id;");
        JoinClause joinClause = (JoinClause) fromTerm.getCorrelateClauses().get(0);
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, joinClause.getTimeTravel().getValueExpression().getKind());
        Assert.assertTrue(joinClause.hasPositionalVariable());
    }

    /**
     * {@code SNAPSHOT} and {@code TIMESTAMP} are not reserved words, but the lookahead that selects the time
     * travel branch keys on them, so neither can name a positional variable. This predates widening the value
     * to an expression; it is pinned here so it stays a decision rather than becoming a surprise.
     */
    @Test
    public void testTimeTravelKeywordCannotNamePositionalVariable() throws Exception {
        assertParseError("SELECT * FROM c AT snapshot;");
        assertParseError("SELECT * FROM c AT timestamp;");
    }

    @Test
    public void testJoinClause() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c JOIN d AT SNAPSHOT 5 - 4 ON c.id = d.id;");
        JoinClause joinClause = (JoinClause) fromTerm.getCorrelateClauses().get(0);
        Assert.assertTrue(joinClause.hasTimeTravel());
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION, joinClause.getTimeTravel().getValueExpression().getKind());
    }

    @Test
    public void testMissingValueIsSyntaxError() throws Exception {
        assertParseError("SELECT * FROM c AT SNAPSHOT;");
        assertParseError("SELECT * FROM c AT TIMESTAMP;");
    }

    @Test
    public void testArithmeticOnSnapshotId() throws Exception {
        Assert.assertEquals(Expression.Kind.OP_EXPRESSION,
                fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT 8574821 + 0;").getValueExpression().getKind());
    }

    /**
     * The value is an expression, so a parenthesised one parses like any other.
     */
    @Test
    public void testParenthesized() throws Exception {
        Assert.assertEquals(TimeTravel.Type.SNAPSHOT_ID,
                fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT (8574821);").getType());
    }

    @Test
    public void testCrossJoin() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c CROSS JOIN d AT SNAPSHOT 5;");
        JoinClause joinClause = (JoinClause) fromTerm.getCorrelateClauses().get(0);
        Assert.assertTrue(joinClause.hasTimeTravel());
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c LEFT OUTER JOIN d AT SNAPSHOT 5 ON c.id = d.id;");
        Assert.assertTrue(((JoinClause) fromTerm.getCorrelateClauses().get(0)).hasTimeTravel());
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c RIGHT OUTER JOIN d AT SNAPSHOT 5 ON c.id = d.id;");
        Assert.assertTrue(((JoinClause) fromTerm.getCorrelateClauses().get(0)).hasTimeTravel());
    }

    /**
     * UNNEST shares the correlate-clause base with JOIN but the grammar never attaches a value
     * to it, so this must remain a syntax error rather than quietly parsing.
     */
    @Test
    public void testUnnestTakesNoTimeTravel() throws Exception {
        assertParseError("SELECT * FROM c UNNEST c.items AS i AT SNAPSHOT 5;");
    }

    @Test
    public void testBothClausesInOneQuery() throws Exception {
        FromTerm fromTerm = fromTerm("SELECT * FROM c AT SNAPSHOT 1 JOIN d AT SNAPSHOT 2 ON c.id = d.id;");
        Assert.assertTrue(fromTerm.hasTimeTravel());
        Assert.assertTrue(((JoinClause) fromTerm.getCorrelateClauses().get(0)).hasTimeTravel());
    }

    /**
     * The visitors read the value without a null check, so a specification must never carry a null one.
     */
    @Test
    public void testSpecRejectsNullValueAndType() throws Exception {
        TimeTravelSpec spec = fromTermTimeTravel("SELECT * FROM c AT SNAPSHOT 1;");
        assertNullRejected(() -> new TimeTravelSpec(null, TimeTravel.Type.SNAPSHOT_ID));
        assertNullRejected(() -> new TimeTravelSpec(spec.getValueExpression(), null));
        assertNullRejected(() -> spec.setValueExpression(null));
    }

    private static void assertNullRejected(Runnable r) {
        try {
            r.run();
            Assert.fail("expected a null to be rejected");
        } catch (NullPointerException expected) {
            // the check this test exists for
        }
    }

    private static String stringLiteral(TimeTravelSpec spec) {
        Expression valueExpr = spec.getValueExpression();
        Assert.assertEquals(Expression.Kind.LITERAL_EXPRESSION, valueExpr.getKind());
        return ((LiteralExpr) valueExpr).getValue().getStringValue();
    }

    private static TimeTravelSpec fromTermTimeTravel(String query) throws Exception {
        FromTerm fromTerm = fromTerm(query);
        Assert.assertTrue("no time travel was parsed", fromTerm.hasTimeTravel());
        return fromTerm.getTimeTravel();
    }

    private static FromTerm fromTerm(String query) throws Exception {
        List<Statement> statements = parse(query);
        Assert.assertEquals(1, statements.size());
        SelectExpression selectExpr = (SelectExpression) ((Query) statements.get(0)).getBody();
        return selectExpr.getSelectSetOperation().getLeftInput().getSelectBlock().getFromClause().getFromTerms().get(0);
    }

    private static List<Statement> parse(String query) throws CompilationException {
        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        IParser parser = factory.createParser(query);
        return parser.parse();
    }

    private static void assertParseError(String query) {
        try {
            parse(query);
            Assert.fail("expected a syntax error for: " + query);
        } catch (CompilationException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Syntax error"));
        }
    }
}
