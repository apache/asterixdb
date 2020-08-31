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

package org.apache.hyracks.algebricks.core.algebra.plan;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.junit.Assert;
import org.junit.Test;

public final class PlanStabilityVerifierTest extends PlanVerifierTestBase {

    final PlanStabilityVerifier verifier = new PlanStabilityVerifier(planPrinter);

    @Test
    public void testVerifySuccess() throws Exception {
        Mutable<ILogicalOperator> opRef1 = createSamplePlan1();
        verifier.recordPlanSignature(opRef1);
        verifier.comparePlanSignature(opRef1);
    }

    @Test
    public void testAddOperator() throws Exception {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        verifier.recordPlanSignature(opRef1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        op1.getInputs().add(newMutable(op2));

        try {
            verifier.comparePlanSignature(opRef1);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
        }
    }

    @Test
    public void testAddOperatorInNestedPlan() throws Exception {
        AssignOperator nestedOp1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));

        SubplanOperator op = new SubplanOperator(nestedOp1);
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        AssignOperator nestedOp2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        nestedOp1.getInputs().add(newMutable(nestedOp2));

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(nestedOp2)));
        }
    }

    @Test
    public void testRemoveOperator() throws Exception {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        op1.getInputs().add(newMutable(op2));

        verifier.recordPlanSignature(opRef1);

        op1.getInputs().clear();

        try {
            verifier.comparePlanSignature(opRef1);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("deleted operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
        }
    }

    @Test
    public void testRemoveOperatorInNestedPlan() throws Exception {
        AssignOperator nestedOp1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));

        AssignOperator nestedOp2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        nestedOp1.getInputs().add(newMutable(nestedOp2));

        SubplanOperator op = new SubplanOperator(nestedOp1);
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        nestedOp1.getInputs().clear();

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("deleted operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(nestedOp2)));
        }
    }

    @Test
    public void testReplaceOperator() throws Exception {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        op1.getInputs().add(newMutable(op2));

        verifier.recordPlanSignature(opRef1);

        AssignOperator op3 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        op1.getInputs().clear();
        op1.getInputs().add(newMutable(op3));

        try {
            verifier.comparePlanSignature(opRef1);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op3)));
        }
    }

    @Test
    public void testReplaceOperatorInNestedPlan() throws Exception {
        AssignOperator nestedOp1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));

        AssignOperator nestedOp2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        nestedOp1.getInputs().add(newMutable(nestedOp2));

        SubplanOperator op = new SubplanOperator(nestedOp1);
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        AssignOperator nestedOp3 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        nestedOp1.getInputs().clear();
        nestedOp1.getInputs().add(newMutable(nestedOp3));

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(nestedOp3)));
        }
    }

    @Test
    public void testAddExpression() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        op.getVariables().add(newVar());
        LogicalVariable testVar = newVar();
        op.getExpressions().add(newMutable(newVarRef(testVar)));

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created expression reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printVar(testVar)));
        }
    }

    @Test
    public void testRemoveExpression() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        op.getVariables().add(newVar());
        LogicalVariable testVar = newVar();
        op.getExpressions().add(newMutable(newVarRef(testVar)));

        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        op.getVariables().remove(1);
        op.getExpressions().remove(1);

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("deleted expression reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printVar(testVar)));
        }
    }

    @Test
    public void testReplaceExpression() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);

        LogicalVariable testVar = newVar();
        op.getExpressions().get(0).setValue(newVarRef(testVar));

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertEquals(String.format("created expression instance (%s)", printVar(testVar)), e.getMessage());
        }
    }

    @Test
    public void testCycleBeforeRecord() {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        Mutable<ILogicalOperator> opRef2 = newMutable(op2);

        op1.getInputs().add(opRef2);
        op2.getInputs().add(opRef1);

        try {
            verifier.recordPlanSignature(opRef1);
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("cycle"));
        }
    }

    @Test
    public void testCycleBeforeCompare() throws Exception {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        Mutable<ILogicalOperator> opRef2 = newMutable(op2);

        op1.getInputs().add(opRef2);

        verifier.recordPlanSignature(opRef1);

        op2.getInputs().add(new MutableObject<>(op1));

        try {
            verifier.comparePlanSignature(opRef1);
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("created operator reference"));
        }
    }

    @Test
    public void testApiDiscard() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(newVarRef(newVar())));
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);
        verifier.comparePlanSignature(opRef);

        verifier.discardPlanSignature();

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + IllegalStateException.class.getName());
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testApiRecordAfterRecordFailure() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(newVarRef(newVar())));
        Mutable<ILogicalOperator> opRef = newMutable(op);

        verifier.recordPlanSignature(opRef);
        verifier.recordPlanSignature(opRef); // ok. the previously recorded state is discarded
        verifier.comparePlanSignature(opRef);
    }

    @Test
    public void testApiCompareWithoutRecordFailure() throws Exception {
        AssignOperator op = newAssign(newVar(), newMutable(newVarRef(newVar())));
        Mutable<ILogicalOperator> opRef = newMutable(op);

        try {
            verifier.comparePlanSignature(opRef);
            Assert.fail("Expected to catch " + IllegalStateException.class.getName());
        } catch (IllegalStateException e) {
            // expected
        }
    }
}
