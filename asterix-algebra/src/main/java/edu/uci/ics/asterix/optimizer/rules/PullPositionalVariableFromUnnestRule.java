/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.RunningAggregatePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnpartitionedPropertyComputer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PullPositionalVariableFromUnnestRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.TID), UnpartitionedPropertyComputer.INSTANCE);
        rOpExprList.add(new MutableObject<ILogicalExpression>(fce));
        RunningAggregateOperator rOp = new RunningAggregateOperator(rOpVars, rOpExprList);
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