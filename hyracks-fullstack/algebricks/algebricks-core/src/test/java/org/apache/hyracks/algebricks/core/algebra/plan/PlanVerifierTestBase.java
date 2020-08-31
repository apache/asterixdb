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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;

public abstract class PlanVerifierTestBase {
    final IPlanPrettyPrinter planPrinter = PlanPrettyPrinter.createStringPlanPrettyPrinter();

    int varCounter;

    EmptyTupleSourceOperator newETS() {
        return new EmptyTupleSourceOperator();
    }

    AssignOperator newAssign(LogicalVariable var, Mutable<ILogicalExpression> exprRef) {
        return new AssignOperator(var, exprRef);
    }

    AssignOperator newAssign(LogicalVariable var1, Mutable<ILogicalExpression> exprRef1, LogicalVariable var2,
            Mutable<ILogicalExpression> exprRef2) {
        AssignOperator op = new AssignOperator(var1, exprRef1);
        op.getVariables().add(var2);
        op.getExpressions().add(exprRef2);
        return op;
    }

    <T> Mutable<T> newMutable(T item) {
        return new MutableObject<>(item);
    }

    VariableReferenceExpression newVarRef(LogicalVariable var) {
        return new VariableReferenceExpression(var);
    }

    LogicalVariable newVar() {
        return new LogicalVariable(varCounter++);
    }

    String printOp(ILogicalOperator op) {
        try {
            return planPrinter.reset().printOperator((AbstractLogicalOperator) op, false).toString();
        } catch (AlgebricksException e) {
            throw new RuntimeException(e);
        }
    }

    String printExpr(ILogicalExpression expr) {
        try {
            return planPrinter.reset().printExpression(expr).toString();
        } catch (AlgebricksException e) {
            throw new RuntimeException(e);
        }
    }

    String printVar(LogicalVariable var) {
        return printExpr(newVarRef(var));
    }

    Mutable<ILogicalOperator> createSamplePlan1() {
        AssignOperator op1 = newAssign(newVar(), newMutable(ConstantExpression.TRUE));

        SubplanOperator op2 = new SubplanOperator(newAssign(newVar(), newMutable(ConstantExpression.TRUE)));
        op1.getInputs().add(newMutable(op2));

        InnerJoinOperator op3 = new InnerJoinOperator(newMutable(ConstantExpression.TRUE));
        op2.getInputs().add(newMutable(op3));

        AssignOperator op4 = new AssignOperator(newVar(), newMutable(ConstantExpression.TRUE));
        op3.getInputs().add(newMutable(op4));

        AssignOperator op5 = new AssignOperator(newVar(), newMutable(ConstantExpression.FALSE));
        op3.getInputs().add(newMutable(op5));

        ReplicateOperator op6 = new ReplicateOperator(2);

        op4.getInputs().add(newMutable(op6));
        op6.getOutputs().add(newMutable(op4));

        op5.getInputs().add(newMutable(op6));
        op6.getOutputs().add(newMutable(op5));

        op6.getInputs().add(newMutable(newETS()));

        return newMutable(op1);
    }
}
