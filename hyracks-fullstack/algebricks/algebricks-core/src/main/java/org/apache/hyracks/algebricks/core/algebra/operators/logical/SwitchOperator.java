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

import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Switch Operator receives an expression and an output mapping. We evaluate the expression during runtime and look up
 * the result in the output mapping. Based on this, we propagate each tuple to the corresponding output branch(es).
 */
public class SwitchOperator extends AbstractReplicateOperator {

    // Expression containing the index of the relevant field
    private final Mutable<ILogicalExpression> branchingExpression;

    // The supplied mapping from field values to arrays of output branch numbers
    private final Map<Integer, int[]> outputMapping;

    public SwitchOperator(int outputArity, Mutable<ILogicalExpression> branchingExpression,
            Map<Integer, int[]> outputMapping) {
        super(outputArity);
        this.branchingExpression = branchingExpression;
        this.outputMapping = outputMapping;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SWITCH;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSwitchOperator(this, arg);
    }

    public Mutable<ILogicalExpression> getBranchingExpression() {
        return branchingExpression;
    }

    public Map<Integer, int[]> getOutputMapping() {
        return outputMapping;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(branchingExpression);
    }
}
