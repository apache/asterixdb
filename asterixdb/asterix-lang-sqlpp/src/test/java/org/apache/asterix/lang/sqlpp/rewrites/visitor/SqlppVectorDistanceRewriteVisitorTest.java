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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.Arrays;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.junit.Assert;
import org.junit.Test;

public class SqlppVectorDistanceRewriteVisitorTest {

    private static final SourceLocation LOC = new SourceLocation(1, 1);

    @Test
    public void rewritesConstantMetricToEuclideanDistance() throws CompilationException {
        CallExpr rewritten = rewrite("[1, 2]", "[3, 4]", "euclidean");
        Assert.assertEquals("euclidean-distance", rewritten.getFunctionSignature().getName());
        Assert.assertEquals(2, rewritten.getFunctionSignature().getArity());
        Assert.assertEquals(2, rewritten.getExprList().size());
    }

    @Test
    public void rewritesConstantMetricToCosineDistance() throws CompilationException {
        CallExpr rewritten = rewrite("[1, 2]", "[3, 4]", "cosine");
        Assert.assertEquals("cosine-distance", rewritten.getFunctionSignature().getName());
    }

    @Test
    public void rejectsNonLiteralMetric() {
        CallExpr callExpr = vectorDistanceCall(list("[1, 2]"), list("[3, 4]"), variable("metric"));
        try {
            new SqlppVectorDistanceRewriteVisitor().visit(callExpr, null);
            Assert.fail("Expected compile-time failure for non-literal metric");
        } catch (CompilationException e) {
            Assert.assertTrue(e.getMessage().contains("compile-time string literal"));
        }
    }

    @Test
    public void rejectsUnknownMetric() {
        CallExpr callExpr = vectorDistanceCall(list("[1, 2]"), list("[3, 4]"), string("unknown"));
        try {
            new SqlppVectorDistanceRewriteVisitor().visit(callExpr, null);
            Assert.fail("Expected compile-time failure for unknown metric");
        } catch (CompilationException e) {
            Assert.assertTrue(e.getMessage().contains("unknown vector_distance metric"));
        }
    }

    @Test
    public void rejectsRemovedMetricAlias() {
        CallExpr callExpr = vectorDistanceCall(list("[1, 2]"), list("[3, 4]"), string("cosine_similarity"));
        try {
            new SqlppVectorDistanceRewriteVisitor().visit(callExpr, null);
            Assert.fail("Expected compile-time failure for removed metric alias");
        } catch (CompilationException e) {
            Assert.assertTrue(e.getMessage().contains("unknown vector_distance metric"));
        }
    }

    private static CallExpr rewrite(String left, String right, String metric) throws CompilationException {
        CallExpr callExpr = vectorDistanceCall(list(left), list(right), string(metric));
        Expression result = new SqlppVectorDistanceRewriteVisitor().visit(callExpr, null);
        return (CallExpr) result;
    }

    private static CallExpr vectorDistanceCall(Expression left, Expression right, Expression metric) {
        CallExpr callExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.VECTOR_DISTANCE),
                Arrays.asList(left, right, metric));
        callExpr.setSourceLocation(LOC);
        return callExpr;
    }

    private static Expression list(String value) {
        LiteralExpr expr = new LiteralExpr(new StringLiteral(value));
        expr.setSourceLocation(LOC);
        return expr;
    }

    private static Expression string(String value) {
        LiteralExpr expr = new LiteralExpr(new StringLiteral(value));
        expr.setSourceLocation(LOC);
        return expr;
    }

    private static Expression variable(String name) {
        VariableExpr expr = new VariableExpr(new VarIdentifier(name));
        expr.setSourceLocation(LOC);
        return expr;
    }
}
