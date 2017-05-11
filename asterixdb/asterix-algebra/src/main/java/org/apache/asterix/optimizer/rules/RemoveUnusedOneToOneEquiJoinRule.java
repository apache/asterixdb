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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes join operators for which all of the following conditions are true:
 * 1. The live variables of one input branch of the join are not used in the upstream plan
 * 2. The join is an inner equi join
 * 3. The join condition only uses variables that correspond to primary keys of the same dataset
 * 4. The records of one input branch will not be filtered by the selective operators till join.
 * Notice that the last condition implies a 1:1 join, i.e., the join does not change the result cardinality.
 * Joins that satisfy the above conditions may be introduced by other rules
 * which use surrogate optimizations. Such an optimization aims to reduce data copies and communication costs by
 * using the primary keys as surrogates for the desired data items. Typically,
 * such a surrogate-based plan introduces a top-level join to finally resolve
 * the surrogates to the desired data items.
 * In case the upstream plan does not require the original data items at all, such a top-level join is unnecessary.
 * The purpose of this rule is to remove such unnecessary joins.
 */
public class RemoveUnusedOneToOneEquiJoinRule implements IAlgebraicRewriteRule {

    private final Set<LogicalVariable> parentsUsedVars = new HashSet<>();
    private final List<LogicalVariable> usedVars = new ArrayList<>();
    private final List<LogicalVariable> liveVars = new ArrayList<>();
    private final List<LogicalVariable> pkVars = new ArrayList<>();
    private final List<DataSourceScanOperator> dataScans = new ArrayList<>();
    private boolean hasRun = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (hasRun) {
            return false;
        }
        hasRun = true;
        if (removeUnusedJoin(opRef)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    private boolean removeUnusedJoin(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean modified = false;

        usedVars.clear();
        VariableUtilities.getUsedVariables(op, usedVars);
        // Propagate used variables from parents downwards.
        parentsUsedVars.addAll(usedVars);

        int numInputs = op.getInputs().size();
        for (int i = 0; i < numInputs; i++) {
            Mutable<ILogicalOperator> childOpRef = op.getInputs().get(i);
            int unusedJoinBranchIndex = removeJoinFromInputBranch(childOpRef);
            if (unusedJoinBranchIndex >= 0) {
                int usedBranchIndex = (unusedJoinBranchIndex == 0) ? 1 : 0;
                // Remove join at input index i, by hooking up op's input i with
                // the join's branch at unusedJoinBranchIndex.
                AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) childOpRef.getValue();
                op.getInputs().set(i, joinOp.getInputs().get(usedBranchIndex));
                modified = true;
            }
            // Descend into children.
            if (removeUnusedJoin(childOpRef)) {
                modified = true;
            }
        }
        return modified;
    }

    private int removeJoinFromInputBranch(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return -1;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        // Make sure the join is an equi-join.
        if (!isEquiJoin(joinOp.getCondition())) {
            return -1;
        }

        int unusedJoinBranchIndex = -1;
        for (int i = 0; i < joinOp.getInputs().size(); i++) {
            liveVars.clear();
            VariableUtilities.getLiveVariables(joinOp.getInputs().get(i).getValue(), liveVars);
            if (liveVars.isEmpty()) {
                // The branch does not produce any variable, i.e., it only contains an empty tuple source.
                return i;
            }
            liveVars.retainAll(parentsUsedVars);
            if (liveVars.isEmpty()) {
                // None of the live variables from this branch are used by its parents.
                unusedJoinBranchIndex = i;
                break;
            }
        }
        if (unusedJoinBranchIndex < 0) {
            // The variables from both branches are used in the upstream plan. We cannot remove this join.
            return -1;
        }

        // Check whether one of the join branches is unused.
        usedVars.clear();
        VariableUtilities.getUsedVariables(joinOp, usedVars);

        // Check whether all used variables originate from primary keys of exactly the same dataset.
        // Collect a list of datascans whose primary key variables are used in the join condition.
        gatherProducingDataScans(opRef, usedVars, dataScans);
        if (dataScans.size() < 2) {
            // Either branch does not use its primary key in the join condition.
            return -1;
        }

        // Check that all datascans scan the same dataset, and that the join condition
        // only used primary key variables of those datascans.
        for (int i = 0; i < dataScans.size(); i++) {
            if (i > 0) {
                DatasetDataSource prevAqlDataSource = (DatasetDataSource) dataScans.get(i - 1).getDataSource();
                DatasetDataSource currAqlDataSource = (DatasetDataSource) dataScans.get(i).getDataSource();
                if (!prevAqlDataSource.getDataset().equals(currAqlDataSource.getDataset())) {
                    return -1;
                }
            }
            // Remove from the used variables all the primary key vars of this dataset.
            fillPKVars(dataScans.get(i), pkVars);
            usedVars.removeAll(pkVars);
        }
        if (!usedVars.isEmpty()) {
            // The join condition also uses some other variables that are not primary
            // keys from datasource scans of the same dataset.
            return -1;
        }
        // Suppose we Project B over A.a ~= B.b, where A's fields are involved in a selective operator.
        // We expect the post-plan will NOT prune the join part derived from A.
        if (unusedJoinBranchIndex >= 0
                && isSelectionAboveDataScan(opRef.getValue().getInputs().get(unusedJoinBranchIndex))) {
            unusedJoinBranchIndex = -1;
        }
        return unusedJoinBranchIndex;
    }

    private boolean isSelectionAboveDataScan(Mutable<ILogicalOperator> opRef) {
        boolean hasSelection = false;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        LogicalOperatorTag tag = op.getOperatorTag();
        switch (tag) {
            case DATASOURCESCAN:
                return false;
            case UNNEST_MAP:
            case LEFT_OUTER_UNNEST_MAP:
            case LIMIT:
            case SELECT:
                return true;
            default:
                for (Mutable<ILogicalOperator> inputOp : op.getInputs()) {
                    hasSelection |= isSelectionAboveDataScan(inputOp);
                }
        }
        return hasSelection;
    }

    private void gatherProducingDataScans(Mutable<ILogicalOperator> opRef, List<LogicalVariable> joinUsedVars,
            List<DataSourceScanOperator> dataScans) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            for (Mutable<ILogicalOperator> inputOp : op.getInputs()) {
                gatherProducingDataScans(inputOp, joinUsedVars, dataScans);
            }
            return;
        }
        DataSourceScanOperator dataScan = (DataSourceScanOperator) op;
        fillPKVars(dataScan, pkVars);
        // Check if join uses all PK vars.
        if (joinUsedVars.containsAll(pkVars)) {
            dataScans.add(dataScan);
        }
    }

    private void fillPKVars(DataSourceScanOperator dataScan, List<LogicalVariable> pkVars) {
        pkVars.clear();
        DatasetDataSource datasetDataSource = (DatasetDataSource) dataScan.getDataSource();
        pkVars.clear();
        if (datasetDataSource.getDataset().getDatasetDetails() instanceof InternalDatasetDetails) {
            int numPKs = datasetDataSource.getDataset().getPrimaryKeys().size();
            for (int i = 0; i < numPKs; i++) {
                pkVars.add(dataScan.getVariables().get(i));
            }
        }
    }

    private boolean isEquiJoin(Mutable<ILogicalExpression> conditionExpr) {
        AbstractLogicalExpression expr = (AbstractLogicalExpression) conditionExpr.getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
            if (funcIdent != AlgebricksBuiltinFunctions.AND && funcIdent != AlgebricksBuiltinFunctions.EQ) {
                return false;
            }
        }
        return true;
    }
}
