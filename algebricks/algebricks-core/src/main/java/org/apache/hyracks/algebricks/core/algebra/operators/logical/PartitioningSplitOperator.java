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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.PartitioningSplitOperatorDescriptor;

/**
 * Partitions it's input based on a given list of expressions.
 * Each expression is assumed to return true/false,
 * and there is exactly one output branch per expression (optionally, plus one default branch).
 * For each input tuple, the expressions are evaluated one-by-one,
 * and the tuple is written to first output branch whose corresponding
 * expression evaluates to true.
 * If all expressions evaluate to false, then
 * the tuple is written to the default output branch, if any was given.
 * If no output branch was given, then such tuples are simply dropped.
 * Given N expressions there may be N or N+1 output branches because the default output branch may be separate from the regular output branches.
 */
public class PartitioningSplitOperator extends AbstractLogicalOperator {

    private final List<Mutable<ILogicalExpression>> expressions;
    private final int defaultBranchIndex;

    public PartitioningSplitOperator(List<Mutable<ILogicalExpression>> expressions, int defaultBranchIndex) throws AlgebricksException {
        this.expressions = expressions;
        this.defaultBranchIndex = defaultBranchIndex;
        // Check that the default output branch index is in [0, N], where N is the number of expressions.
        if (defaultBranchIndex != PartitioningSplitOperatorDescriptor.NO_DEFAULT_BRANCH
                && defaultBranchIndex > expressions.size()) {
            throw new AlgebricksException("Default branch index out of bounds. Number of exprs given: "
                    + expressions.size() + ". The maximum default branch index may therefore be: " + expressions.size());
        }
    }

    public List<Mutable<ILogicalExpression>> getExpressions() {
        return expressions;
    }

    public int getDefaultBranchIndex() {
        return defaultBranchIndex;
    }
    
    public int getNumOutputBranches() {
        return (defaultBranchIndex == expressions.size()) ? expressions.size() + 1 : expressions.size();
    }
    
    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.PARTITIONINGSPLIT;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        for (int i = 0; i < expressions.size(); i++) {
            if (visitor.transform(expressions.get(i))) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitPartitioningSplitOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

}