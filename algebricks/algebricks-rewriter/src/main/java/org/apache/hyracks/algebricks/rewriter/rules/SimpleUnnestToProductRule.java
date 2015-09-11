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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class SimpleUnnestToProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN
                && op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();

        if (!(op2 instanceof AbstractScanOperator) && !descOrSelfIsSourceScan(op2)) {
            return false;
        }
        // Make sure that op does not use any variables produced by op2.
        if (!opsAreIndependent(op, op2)) {
            return false;
        }

        /**
         * finding the boundary between left branch and right branch
         * operator pipeline on-top-of boundaryOpRef (exclusive) is the inner branch
         * operator pipeline under boundaryOpRef (inclusive) is the outer branch
         */
        Mutable<ILogicalOperator> currentOpRef = opRef;
        Mutable<ILogicalOperator> boundaryOpRef = currentOpRef.getValue().getInputs().get(0);
        while (currentOpRef.getValue().getInputs().size() == 1) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        Mutable<ILogicalOperator> tupleSourceOpRef = currentOpRef;
        currentOpRef = opRef;
        if (tupleSourceOpRef.getValue().getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) tupleSourceOpRef.getValue();
            // If the subplan input is a trivial plan, do not do the rewriting.
            if (nts.getSourceOperator().getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                while (currentOpRef.getValue().getInputs().size() == 1
                        && currentOpRef.getValue() instanceof AbstractScanOperator
                        && descOrSelfIsSourceScan((AbstractLogicalOperator) currentOpRef.getValue())) {
                    if (opsAreIndependent(currentOpRef.getValue(), tupleSourceOpRef.getValue())) {
                        /** move down the boundary if the operator is independent of the tuple source */
                        boundaryOpRef = currentOpRef.getValue().getInputs().get(0);
                    } else {
                        break;
                    }
                    currentOpRef = currentOpRef.getValue().getInputs().get(0);
                }
            }
        }

        /** join the two independent branches */
        InnerJoinOperator join = new InnerJoinOperator(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE),
                new MutableObject<ILogicalOperator>(boundaryOpRef.getValue()), new MutableObject<ILogicalOperator>(
                        opRef.getValue()));
        opRef.setValue(join);
        ILogicalOperator ets = new EmptyTupleSourceOperator();
        boundaryOpRef.setValue(ets);
        context.computeAndSetTypeEnvironmentForOperator(boundaryOpRef.getValue());
        context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        context.computeAndSetTypeEnvironmentForOperator(join);
        return true;
    }

    private boolean descOrSelfIsSourceScan(AbstractLogicalOperator op2) {
        // Disregard data source scans in a subplan.
        if (op2.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        if (op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN
                && op2.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return true;
        }
        for (Mutable<ILogicalOperator> cRef : op2.getInputs()) {
            AbstractLogicalOperator alo = (AbstractLogicalOperator) cRef.getValue();
            if (descOrSelfIsSourceScan(alo)) {
                return true;
            }
        }
        return false;
    }

    private boolean opsAreIndependent(ILogicalOperator unnestOp, ILogicalOperator outer) throws AlgebricksException {
        if (unnestOp.equals(outer)) {
            return false;
        }
        List<LogicalVariable> opUsedVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(unnestOp, opUsedVars);
        Set<LogicalVariable> op2LiveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(outer, op2LiveVars);
        for (LogicalVariable usedVar : opUsedVars) {
            if (op2LiveVars.contains(usedVar)) {
                return false;
            }
        }
        return true;
    }

}
