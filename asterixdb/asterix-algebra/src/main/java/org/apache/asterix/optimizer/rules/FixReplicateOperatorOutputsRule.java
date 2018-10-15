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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule does not really rewrite the plan. The whole purpose of this rule is to fix the outputs of replicate/split
 * operator. AbstractReplicateOperator has a list of outputs. The outputs represent the logical operators to which the
 * replicate operator is connected to. Ideally, the outputs should match the parents of replicate operator in the real
 * execution plan. However, the issue is that when the replicate operator is created, it starts out with specific
 * outputs as its parents. Afterwards, while optimizing the plan, new operators could be inserted above replicate
 * operators. In the plan now, those operators have their inputs coming from replicate operator. But the replicate
 * operator still holds the "old" parents as outputs. One example is when bulk loading into a dataset with some indexes.
 * At first, the plan looks something like this:
 * ...              ...
 * |                |
 * idx1 insert Op   idx2 insert Op
 * \________________/
 *          |
 *       replicate (where replicate.outputs = idx1 insert operator & idx2 insert operator
 *
 * After several optimizations, the plan would look something like this:
 * ...              ...
 * |                |
 * idx1 insert Op   idx2 insert Op
 * |                |
 * ...              ...
 * |                |
 * exchange1        exchange2
 * \________________/
 *          |
 *       replicate (where replicate.outputs is still = idx1 insert operator & idx2 insert operator)
 *
 * The reason for this divergence is that the usual routine when inserting a new operator, like exchange1 in the plan,
 * exchange1 operator sets its input to the operator below it which is a one way change for operators that have outputs
 * as instance variables such as replicate/split.
 */
public class FixReplicateOperatorOutputsRule implements IAlgebraicRewriteRule {

    // Integer = how many parents have been fixed
    private final Map<AbstractReplicateOperator, MutableInt> replicateOperators;
    private final List<Mutable<ILogicalOperator>> parentsPathToReplicate;

    public FixReplicateOperatorOutputsRule() {
        parentsPathToReplicate = new ArrayList<>();
        replicateOperators = new HashMap<>();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        // as you traverse down the tree, append the operators on your path
        parentsPathToReplicate.add(opRef);
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // remove yourself from the path since you should be the last one by now
        parentsPathToReplicate.remove(parentsPathToReplicate.size() - 1);

        // when done with the whole plan, check that all replicate operators have been fixed
        // if there is one that has not been completely fixed, it indicates that one "old" parent couldn't be found
        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT
                || op.getOperatorTag() == LogicalOperatorTag.SINK
                || (op.getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR
                        && ((DelegateOperator) op).getDelegate() instanceof CommitOperator)) {
            for (Map.Entry<AbstractReplicateOperator, MutableInt> entry : replicateOperators.entrySet()) {
                if (entry.getKey().getOutputs().size() != entry.getValue().getValue()) {
                    throw new CompilationException(ErrorCode.COMPILATION_FAILED_DUE_TO_REPLICATE_OP,
                            op.getSourceLocation());
                }
            }
            return false;
        }

        // rewrite/fix only replicate operators
        if ((op.getOperatorTag() != LogicalOperatorTag.REPLICATE && op.getOperatorTag() != LogicalOperatorTag.SPLIT)
                || context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        AbstractReplicateOperator replicateOperator = (AbstractReplicateOperator) op;

        // initialize number of parents fixed for this replicate operator
        replicateOperators.putIfAbsent(replicateOperator, new MutableInt(0));

        // fix the old parent of replicate operator, the real parent is the last element in parentsPathToReplicate
        Mutable<ILogicalOperator> replicateActualParent = parentsPathToReplicate.get(parentsPathToReplicate.size() - 1);

        // if the real parent is already in the output list of the replicate, do nothing
        if (replicateOperator.getOutputs().contains(replicateActualParent)) {
            // update number of parents done for this replicate operator
            updateNumberOfParentsDone(replicateOperator);

            // if all parents are fixed, add this replicate to the do not apply set
            if (replicateOperators.get(replicateOperator).getValue() == replicateOperator.getOutputs().size()) {
                context.addToDontApplySet(this, opRef.getValue());
            }
            return false;
        } else {
            // if the parent (the one currently in the plan) is not in the output list of the replicate operator,
            // the "old" output (one that was once the parent) should be replaced with the actual parent in the plan
            // find this old parent in the parentsPathToReplicate
            boolean parentFixed = false;
            for (int oldParentIndex = 0; oldParentIndex < replicateOperator.getOutputs().size(); oldParentIndex++) {
                if (parentsPathToReplicate.contains(replicateOperator.getOutputs().get(oldParentIndex))) {
                    replicateOperator.getOutputs().set(oldParentIndex, replicateActualParent);
                    parentFixed = true;
                    updateNumberOfParentsDone(replicateOperator);
                    break;
                }
            }

            // if all parents are fixed, add this replicate to the do not apply set
            if (replicateOperators.get(replicateOperator).getValue() == replicateOperator.getOutputs().size()) {
                context.addToDontApplySet(this, opRef.getValue());
            }

            return parentFixed;
        }
    }

    /**
     * replicate operator could have more than one parent (output). This method keeps count of how many outputs
     * have been fixed
     * @param replicateOperator the replicate operator in question
     */
    private void updateNumberOfParentsDone(AbstractReplicateOperator replicateOperator) {
        MutableInt numParentsDone = replicateOperators.get(replicateOperator);
        Integer newNumParentsDone = numParentsDone.getValue() + 1;
        numParentsDone.setValue(newNumParentsDone);
    }
}
