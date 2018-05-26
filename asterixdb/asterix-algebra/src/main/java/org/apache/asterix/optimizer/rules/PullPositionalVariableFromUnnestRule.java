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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RunningAggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.UnpartitionedPropertyComputer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PullPositionalVariableFromUnnestRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;
        LogicalVariable p = unnest.getPositionalVariable();
        if (p == null) {
            return false;
        }
        ArrayList<LogicalVariable> rOpVars = new ArrayList<LogicalVariable>();
        rOpVars.add(p);
        ArrayList<Mutable<ILogicalExpression>> rOpExprList = new ArrayList<Mutable<ILogicalExpression>>();
        StatefulFunctionCallExpression fce = new StatefulFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.TID), UnpartitionedPropertyComputer.INSTANCE);
        fce.setSourceLocation(op.getSourceLocation());
        rOpExprList.add(new MutableObject<ILogicalExpression>(fce));
        RunningAggregateOperator rOp = new RunningAggregateOperator(rOpVars, rOpExprList);
        rOp.setSourceLocation(unnest.getSourceLocation());
        rOp.setExecutionMode(unnest.getExecutionMode());
        RunningAggregatePOperator rPop = new RunningAggregatePOperator();
        rOp.setPhysicalOperator(rPop);
        rOp.getInputs().add(new MutableObject<ILogicalOperator>(unnest));
        opRef.setValue(rOp);
        unnest.setPositionalVariable(null);
        context.computeAndSetTypeEnvironmentForOperator(rOp);
        context.computeAndSetTypeEnvironmentForOperator(unnest);
        return true;
    }
}
