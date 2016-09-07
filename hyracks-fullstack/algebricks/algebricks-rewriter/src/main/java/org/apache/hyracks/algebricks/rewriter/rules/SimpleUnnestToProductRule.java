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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class SimpleUnnestToProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (!isScanOrUnnest(op)) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        ILogicalOperator op2 = opRef2.getValue();
        if (!isScanOrUnnest(op2) && !descOrSelfIsSourceScan(op2)) {
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
            while (currentOpRef.getValue().getInputs().size() == 1
                    /*
                     * When this rule is fired,
                     * Unnests with a dataset function have been rewritten to DataSourceScans and
                     * AccessMethod related rewriting hasn't been done. Therefore, we only need
                     * to check if currentOpRef holds a DataSourceScanOperator.
                     */
                    && currentOpRef.getValue().getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN
                    && descOrSelfIsSourceScan(currentOpRef.getValue())) {
                if (opsAreIndependent(currentOpRef.getValue(), tupleSourceOpRef.getValue())) {
                    /** move down the boundary if the operator is independent of the tuple source */
                    boundaryOpRef = currentOpRef.getValue().getInputs().get(0);
                } else {
                    break;
                }
                currentOpRef = currentOpRef.getValue().getInputs().get(0);
            }
        } else {
            //Move the boundary below any top const assigns.
            boundaryOpRef = opRef.getValue().getInputs().get(0);
            while (boundaryOpRef.getValue().getInputs().size() == 1
                    /*
                     * When this rule is fired,
                     * Unnests with a dataset function have been rewritten to DataSourceScans and
                     * AccessMethod related rewriting hasn't been done. Therefore, we only need
                     * to check if boundaryOpRef holds a DataSourceScanOperator.
                     */
                    && boundaryOpRef.getValue().getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
                List<LogicalVariable> opUsedVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getUsedVariables(boundaryOpRef.getValue(), opUsedVars);
                if (opUsedVars.size() == 0 && OperatorPropertiesUtil.isMovable(boundaryOpRef.getValue())
                /* We cannot freely move the location of operators tagged as un-movable. */) {
                    // move down the boundary if the operator is a const assigns.
                    boundaryOpRef = boundaryOpRef.getValue().getInputs().get(0);
                } else {
                    break;
                }
            }
        }

        // If the left branch has cardinality one, we do not need to rewrite the unary pipeline
        // into a cartesian product.
        ILogicalOperator innerBranchOperator = opRef.getValue();
        ILogicalOperator boundaryOperator = boundaryOpRef.getValue();
        if (OperatorPropertiesUtil.isCardinalityZeroOrOne(boundaryOperator)
                // Note that for an external data source, the following check returns false.
                // Thus, it does not produce absolutely correct plan for the CASE expression
                // that contains subqueries over external datasets in THEN/ELSE branches, in
                // the sense that if the condition is not satisfied, the corresponding THEN/ELSE
                // branch should not be evaluated at all. Rewriting to a join will actually
                // evaluates those branches.
                // Fixing ASTERIXDB-1620 will ensure correctness for external datasets.
                && !descOrSelfIsLeafSourceScan(innerBranchOperator, boundaryOperator)) {
            return false;
        }

        /** join the two independent branches */
        InnerJoinOperator join = new InnerJoinOperator(new MutableObject<>(ConstantExpression.TRUE),
                new MutableObject<>(boundaryOperator), new MutableObject<>(innerBranchOperator));
        opRef.setValue(join);
        ILogicalOperator ets = new EmptyTupleSourceOperator();
        context.computeAndSetTypeEnvironmentForOperator(ets);
        boundaryOpRef.setValue(ets);
        context.computeAndSetTypeEnvironmentForOperator(boundaryOperator);
        context.computeAndSetTypeEnvironmentForOperator(innerBranchOperator);
        context.computeAndSetTypeEnvironmentForOperator(join);
        return true;
    }

    private boolean descOrSelfIsSourceScan(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            return true;
        }
        for (Mutable<ILogicalOperator> cRef : op.getInputs()) {
            if (descOrSelfIsSourceScan(cRef.getValue())) {
                return true;
            }
        }
        return false;
    }

    private boolean descOrSelfIsLeafSourceScan(ILogicalOperator op, ILogicalOperator bottomOp) {
        if (op == bottomOp) {
            return false;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataSourceScanOperator = (DataSourceScanOperator) op;
            return dataSourceScanOperator.getDataSource().isScanAccessPathALeaf();
        }
        for (Mutable<ILogicalOperator> cRef : op.getInputs()) {
            if (descOrSelfIsLeafSourceScan(cRef.getValue(), bottomOp)) {
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

    private boolean isScanOrUnnest(ILogicalOperator op) {
        LogicalOperatorTag opTag = op.getOperatorTag();
        return opTag == LogicalOperatorTag.DATASOURCESCAN || opTag == LogicalOperatorTag.UNNEST;
    }

}
