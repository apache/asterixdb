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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes join operators for which all of the following conditions are true:
 * 1. The live variables of one input branch of the join are not used in the upstream plan
 * 2. The join is an inner equi join
 * 3. The join condition only uses variables that correspond to primary keys of the same dataset
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

    private final Set<LogicalVariable> parentsUsedVars = new HashSet<LogicalVariable>();
    private final List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
    private final List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
    private final List<LogicalVariable> pkVars = new ArrayList<LogicalVariable>();
    private final List<DataSourceScanOperator> dataScans = new ArrayList<DataSourceScanOperator>();
    private boolean hasRun = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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

        // Check that all datascans scan the same dataset, and that the join condition
        // only used primary key variables of those datascans.
        for (int i = 0; i < dataScans.size(); i++) {
            if (i > 0) {
                AqlDataSource prevAqlDataSource = (AqlDataSource) dataScans.get(i - 1).getDataSource();
                AqlDataSource currAqlDataSource = (AqlDataSource) dataScans.get(i).getDataSource();
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
        return unusedJoinBranchIndex;
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
        AqlDataSource aqlDataSource = (AqlDataSource) dataScan.getDataSource();
        pkVars.clear();
        if (aqlDataSource.getDataset().getDatasetDetails() instanceof InternalDatasetDetails) {
            int numPKs = DatasetUtils.getPartitioningKeys(aqlDataSource.getDataset()).size();
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
