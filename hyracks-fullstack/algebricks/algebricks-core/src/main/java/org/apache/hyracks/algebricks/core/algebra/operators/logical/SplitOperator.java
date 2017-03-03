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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Split Operator receives an expression. This expression will be evaluated to an integer value during the runtime.
 * Based on its value, it propagates each tuple to the corresponding output frame. (e.g., first output = 0, ...)
 * Thus, unlike Replicate operator that does unconditional propagation to all outputs,
 * this does a conditional propagate operation.
 */
public class SplitOperator extends AbstractReplicateOperator {

    // Expression that keeps the output branch information for each tuple
    private final Mutable<ILogicalExpression> branchingExpression;
    // Default branch when there is no value from the given branching expression. The default is 0.
    private final int defaultBranch;
    // When the following is set to true, defaultBranch will be ignored and incoming tuples will be
    // propagated to all output branches. The default is false.
    private final boolean propageToAllBranchAsDefault;

    public SplitOperator(int outputArity, Mutable<ILogicalExpression> branchingExpression) {
        this(outputArity, branchingExpression, 0, false);
    }

    public SplitOperator(int outputArity, Mutable<ILogicalExpression> branchingExpression, int defaultBranch,
            boolean propageToAllBranchForMissingExprValue) {
        super(outputArity);
        this.branchingExpression = branchingExpression;
        this.defaultBranch = defaultBranch;
        this.propageToAllBranchAsDefault = propageToAllBranchForMissingExprValue;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SPLIT;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSplitOperator(this, arg);
    }

    public Mutable<ILogicalExpression> getBranchingExpression() {
        return branchingExpression;
    }

    public int getDefaultBranch() {
        return defaultBranch;
    }

    public boolean getPropageToAllBranchAsDefault() {
        return propageToAllBranchAsDefault;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(branchingExpression);
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        getBranchingExpression().getValue().substituteVar(v1, v2);
    }

}
