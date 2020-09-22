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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.junit.Assert;
import org.junit.Test;

public final class PlanStructureVerifierTest extends PlanVerifierTestBase implements ITypingContext {

    final PlanStructureVerifier verifier = new PlanStructureVerifier(planPrinter, this);

    @Test
    public void testVerifySuccess() throws Exception {
        Mutable<ILogicalOperator> opRef1 = createSamplePlan1();
        verifier.verifyPlanStructure(opRef1);
    }

    @Test
    public void testSharedExpressionReferenceInSameOp() {
        LogicalVariable v1 = newVar();
        AssignOperator op1 = newAssign(v1, newMutable(ConstantExpression.TRUE));

        Mutable<ILogicalExpression> v1Ref = newMutable(newVarRef(v1));

        AssignOperator op2 = newAssign(newVar(), v1Ref, newVar(), v1Ref);
        op2.getInputs().add(newMutable(op1));

        try {
            verifier.verifyPlanStructure(newMutable(op2));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared expression reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("to (" + printVar(v1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("in " + printOp(op2)));
        }
    }

    @Test
    public void testSharedExpressionInSameOp() {
        LogicalVariable v1 = newVar();
        AssignOperator op1 = newAssign(v1, newMutable(ConstantExpression.TRUE));

        ILogicalExpression v1Ref = newVarRef(v1);

        AssignOperator op2 = newAssign(newVar(), newMutable(v1Ref), newVar(), newMutable(v1Ref));
        op2.getInputs().add(newMutable(op1));

        try {
            verifier.verifyPlanStructure(newMutable(op2));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared expression instance"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("(" + printVar(v1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("in " + printOp(op2)));
        }
    }

    @Test
    public void testSharedExpressionReferenceBetweenOps() {
        LogicalVariable v1 = newVar();
        AssignOperator op1 = newAssign(v1, newMutable(ConstantExpression.TRUE));

        Mutable<ILogicalExpression> v1Ref = newMutable(newVarRef(v1));

        AssignOperator op2 = newAssign(newVar(), v1Ref);
        op2.getInputs().add(newMutable(op1));

        AssignOperator op3 = newAssign(newVar(), v1Ref);
        op3.getInputs().add(newMutable(op2));

        try {
            verifier.verifyPlanStructure(newMutable(op3));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared expression reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("to (" + printVar(v1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op3)));
        }
    }

    @Test
    public void testSharedExpressionBetweenOps() {
        LogicalVariable v1 = newVar();
        AssignOperator op1 = newAssign(v1, newMutable(ConstantExpression.TRUE));

        ILogicalExpression v1Ref = newVarRef(v1);

        AssignOperator op2 = newAssign(newVar(), newMutable(v1Ref));
        op2.getInputs().add(newMutable(op1));

        AssignOperator op3 = newAssign(newVar(), newMutable(v1Ref));
        op3.getInputs().add(newMutable(op2));

        try {
            verifier.verifyPlanStructure(newMutable(op3));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared expression instance"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("(" + printVar(v1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op3)));
        }
    }

    @Test
    public void testSharedOperatorReferenceInSameOp() {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        InnerJoinOperator op2 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));
        op2.getInputs().add(opRef1);
        op2.getInputs().add(opRef1);

        try {
            verifier.verifyPlanStructure(newMutable(op2));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("to (" + printOp(op1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("in " + printOp(op2)));
        }
    }

    @Test
    public void testSharedOperatorInSameOp() {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));

        InnerJoinOperator op2 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));
        op2.getInputs().add(newMutable(op1));
        op2.getInputs().add(newMutable(op1));

        try {
            verifier.verifyPlanStructure(newMutable(op2));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared operator instance"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("(" + printOp(op1) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("in " + printOp(op2)));
        }
    }

    @Test
    public void testSharedOperatorReferenceBetweenOps() {
        InnerJoinOperator op1 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));

        InnerJoinOperator op2 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));
        op1.getInputs().add(newMutable(op2));

        InnerJoinOperator op3 = new InnerJoinOperator(newMutable(ConstantExpression.FALSE));
        op1.getInputs().add(newMutable(op3));

        AssignOperator op4 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        Mutable<ILogicalOperator> opRef4 = newMutable(op4);

        op2.getInputs().add(opRef4);
        op2.getInputs().add(newMutable(newAssign(newVar(), newMutable(ConstantExpression.MISSING))));

        op3.getInputs().add(opRef4);
        op3.getInputs().add(newMutable(newAssign(newVar(), newMutable(ConstantExpression.MISSING))));

        try {
            verifier.verifyPlanStructure(newMutable(op1));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared operator reference"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("to (" + printOp(op4) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op3)));
        }
    }

    @Test
    public void testSharedOperatorBetweenOps() {
        InnerJoinOperator op1 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));

        InnerJoinOperator op2 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));
        op1.getInputs().add(newMutable(op2));

        InnerJoinOperator op3 = new InnerJoinOperator(newMutable(ConstantExpression.FALSE));
        op1.getInputs().add(newMutable(op3));

        AssignOperator op4 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));

        op2.getInputs().add(newMutable(op4));
        op2.getInputs().add(newMutable(newAssign(newVar(), newMutable(ConstantExpression.MISSING))));

        op3.getInputs().add(newMutable(op4));
        op3.getInputs().add(newMutable(newAssign(newVar(), newMutable(ConstantExpression.MISSING))));

        try {
            verifier.verifyPlanStructure(newMutable(op1));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("shared operator instance"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("(" + printOp(op4) + ")"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op2)));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(printOp(op3)));
        }
    }

    @Test
    public void testCycle() {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        Mutable<ILogicalOperator> opRef1 = newMutable(op1);

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        Mutable<ILogicalOperator> opRef2 = newMutable(op2);

        op1.getInputs().add(opRef2);
        op2.getInputs().add(opRef1);

        try {
            verifier.verifyPlanStructure(opRef1);
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("cycle"));
        }
    }

    @Test
    public void testNoSchema() {
        EmptyTupleSourceOperator ets = newETS();
        ets.recomputeSchema();

        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));
        op1.getInputs().add(newMutable(ets));
        op1.recomputeSchema();

        op1.getInputs().clear();

        AssignOperator op2 = newAssign(newVar(), newMutable(ConstantExpression.FALSE));
        // no schema
        op1.getInputs().add(newMutable(op2));

        try {
            verifier.verifyPlanStructure(newMutable(op1));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("missing schema"));
        }
    }

    @Test
    public void testNoTypeEnvironment() throws Exception {
        EmptyTupleSourceOperator ets = newETS();
        computeAndSetTypeEnvironmentForOperator(ets);
        ets.recomputeSchema();

        SelectOperator op1 = new SelectOperator(newMutable(ConstantExpression.TRUE), false, null);
        op1.getInputs().add(newMutable(ets));
        computeAndSetTypeEnvironmentForOperator(op1);
        op1.recomputeSchema();

        op1.getInputs().clear();

        SelectOperator op2 = new SelectOperator(newMutable(ConstantExpression.FALSE), false, null);
        op2.getInputs().add(newMutable(ets));
        op2.recomputeSchema();
        // no type env

        op1.getInputs().add(newMutable(op2));

        try {
            verifier.verifyPlanStructure(newMutable(op1));
            Assert.fail("Expected to catch " + AlgebricksException.class.getName());
        } catch (AlgebricksException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("missing output type environment"));
        }
    }

    // ITypingContext

    final Map<ILogicalOperator, IVariableTypeEnvironment> typeEnvMap = new HashMap<>();

    @Override
    public void computeAndSetTypeEnvironmentForOperator(ILogicalOperator op) throws AlgebricksException {
        setOutputTypeEnvironment(op, op.computeOutputTypeEnvironment(this));
    }

    @Override
    public void setOutputTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env) {
        typeEnvMap.put(op, env);
    }

    @Override
    public IVariableTypeEnvironment getOutputTypeEnvironment(ILogicalOperator op) {
        return typeEnvMap.get(op);
    }

    @Override
    public void invalidateTypeEnvironmentForOperator(ILogicalOperator op) {
        typeEnvMap.remove(op);
    }

    @Override
    public IExpressionTypeComputer getExpressionTypeComputer() {
        return null;
    }

    @Override
    public IMissableTypeComputer getMissableTypeComputer() {
        return null;
    }

    @Override
    public IConflictingTypeResolver getConflictingTypeResolver() {
        return null;
    }

    @Override
    public IMetadataProvider<?, ?> getMetadataProvider() {
        return null;
    }
}
