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
package org.apache.asterix.optimizer.rules.subplan;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

import com.google.common.collect.ImmutableSet;

class SubplanFlatteningUtil {

    /**
     * Blindly inline all NTS's in a Subplan operator.
     *
     * @param subplanOp,
     *            the subplan operator
     * @param context
     * @return a map that maps primary key variables in the subplan's input to its deep copies
     *         in the nested pipeline; the ordering that needs to be maintained for the final
     *         aggregation in the added group-by operator.
     * @throws AlgebricksException
     */
    public static Pair<Map<LogicalVariable, LogicalVariable>, List<Pair<IOrder, Mutable<ILogicalExpression>>>> inlineAllNestedTupleSource(
            SubplanOperator subplanOp, IOptimizationContext context) throws AlgebricksException {
        // For nested subplan, we do not continue for the general inlining.
        if (OperatorManipulationUtil.ancestorOfOperators(subplanOp,
                ImmutableSet.of(LogicalOperatorTag.NESTEDTUPLESOURCE))) {
            return new Pair<Map<LogicalVariable, LogicalVariable>, List<Pair<IOrder, Mutable<ILogicalExpression>>>>(
                    null, null);
        }
        InlineAllNtsInSubplanVisitor visitor = new InlineAllNtsInSubplanVisitor(context, subplanOp);

        // Rewrites the query plan.
        ILogicalOperator topOp = findLowestAggregate(subplanOp.getNestedPlans().get(0).getRoots().get(0)).getValue();
        ILogicalOperator opToVisit = topOp.getInputs().get(0).getValue();
        ILogicalOperator result = opToVisit.accept(visitor, null);
        topOp.getInputs().get(0).setValue(result);

        // Substitute variables in topOp if necessary.
        VariableUtilities.substituteVariables(topOp, visitor.getVariableMapHistory(), context);

        // Gets ordering variables.
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderVars = visitor.getOrderingExpressions();
        return new Pair<Map<LogicalVariable, LogicalVariable>, List<Pair<IOrder, Mutable<ILogicalExpression>>>>(
                visitor.getInputVariableToOutputVariableMap(), orderVars);
    }

    /**
     * Inline the left NTS in a subplan that satisfies a special condition indicated
     * by canFlattenSubplanJoinRuleFire(...).
     *
     * @param subplanOp
     *            the SubplanOperator
     * @param context
     *            the optimization context
     * @return A set of variables used for further null-checks, i.e., variables indicating
     *         whether a tuple produced by a transformed left outer join is a non-match;
     *         a reference to the top join operator in the nested subplan.
     * @throws AlgebricksException
     */
    public static Pair<Set<LogicalVariable>, Mutable<ILogicalOperator>> inlineLeftNtsInSubplanJoin(
            SubplanOperator subplanOp, IOptimizationContext context) throws AlgebricksException {
        Pair<Boolean, ILogicalOperator> applicableAndNtsToRewrite =
                SubplanFlatteningUtil.isQualifiedForSpecialFlattening(subplanOp);
        if (!applicableAndNtsToRewrite.first) {
            return new Pair<Set<LogicalVariable>, Mutable<ILogicalOperator>>(null, null);
        }

        ILogicalOperator qualifiedNts = applicableAndNtsToRewrite.second;
        ILogicalOperator subplanInputOp = subplanOp.getInputs().get(0).getValue();
        InlineLeftNtsInSubplanJoinFlatteningVisitor specialVisitor =
                new InlineLeftNtsInSubplanJoinFlatteningVisitor(context, subplanInputOp, qualifiedNts);

        // Rewrites the query plan.
        Mutable<ILogicalOperator> topRef = subplanOp.getNestedPlans().get(0).getRoots().get(0);
        ILogicalOperator result = topRef.getValue().accept(specialVisitor, null); // The special visitor doesn't replace any input or local variables.
        Mutable<ILogicalOperator> topJoinRef = specialVisitor.getTopJoinReference();
        topRef.setValue(result);

        // Inline the rest Nts's as general cases.
        InlineAllNtsInSubplanVisitor generalVisitor = new InlineAllNtsInSubplanVisitor(context, subplanOp);
        ILogicalOperator opToVisit = topJoinRef.getValue();
        result = opToVisit.accept(generalVisitor, null);
        topJoinRef.setValue(result);

        // Substitute variables in nested pipeline above the top join operator in the nested pipeline if necessary.
        List<Pair<LogicalVariable, LogicalVariable>> subplanLocalVarMap = generalVisitor.getVariableMapHistory();
        ILogicalOperator currentOp = topRef.getValue();
        while (currentOp != result) {
            VariableUtilities.substituteVariables(currentOp, subplanLocalVarMap, context);
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        return new Pair<Set<LogicalVariable>, Mutable<ILogicalOperator>>(specialVisitor.getNullCheckVariables(),
                topJoinRef);
    }

    /**
     * @param subplanOp
     *            a SubplanOperator
     * @return whether there is a data source scan in the nested logical plans of {@code subplanOp}.
     */
    public static boolean containsOperators(SubplanOperator subplanOp, Set<LogicalOperatorTag> interestedOperatorTags) {
        List<ILogicalPlan> nestedPlans = subplanOp.getNestedPlans();
        for (ILogicalPlan nestedPlan : nestedPlans) {
            for (Mutable<ILogicalOperator> opRef : nestedPlan.getRoots()) {
                if (containsOperatorsInternal(opRef.getValue(), interestedOperatorTags)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether the query plan rooted {@code currentOp} contains a data source scan operator,
     * with considering nested subplans.
     *
     * @param currentOp
     *            the current operator
     * @return true if {@code currentOp} contains a data source scan operator; false otherwise.
     */
    public static boolean containsOperatorsInternal(ILogicalOperator currentOp,
            Set<LogicalOperatorTag> interestedOperatorTags) {
        if (interestedOperatorTags.contains(currentOp.getOperatorTag())) {
            return true;
        }
        if (currentOp.getOperatorTag() == LogicalOperatorTag.SUBPLAN
                && containsOperators((SubplanOperator) currentOp, interestedOperatorTags)) {
            return true;
        }
        for (Mutable<ILogicalOperator> childRef : currentOp.getInputs()) {
            if (containsOperatorsInternal(childRef.getValue(), interestedOperatorTags)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds the lowest aggregate operator that should be put into a nested aggregation pipeline within a group-by
     * operator.
     * <p/>
     * Note that neither binary input operators nor data scan can be put into a group by operator.
     *
     * @param currentOpRef,
     *            the current root operator reference to look at.
     * @return the operator reference of the lowest qualified aggregate operator.
     */
    public static Mutable<ILogicalOperator> findLowestAggregate(Mutable<ILogicalOperator> currentOpRef) {
        ILogicalOperator currentOp = currentOpRef.getValue();
        // Neither binary input operators nor data scan can be put into a group by operator.
        if (currentOp.getInputs().size() != 1 || currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            return null;
        }
        Mutable<ILogicalOperator> childReturn = findLowestAggregate(currentOp.getInputs().get(0));
        if (childReturn == null) {
            return currentOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE ? currentOpRef : null;
        }
        return childReturn;
    }

    /**
     * Determine whether a subplan could be rewritten as a join-related special case.
     * The conditions include:
     * a. there is a join (let's call it J1.) in the nested plan,
     * b. if J1 is an inner join, one input pipeline of J1 has a NestedTupleSource descendant (let's call it N1),
     * c. if J1 is a left outer join, the left branch of J1 has a NestedTupleSource descendant (let's call it N1),
     * d. there is no tuple dropping from N1 to J1.
     *
     * @param subplanOp,
     *            the SubplanOperator to consider
     * @return TRUE if the rewriting is applicable; FALSE otherwise.
     * @throws AlgebricksException
     */
    private static Pair<Boolean, ILogicalOperator> isQualifiedForSpecialFlattening(SubplanOperator subplanOp)
            throws AlgebricksException {
        if (!OperatorManipulationUtil.ancestorOfOperators(
                subplanOp.getNestedPlans().get(0).getRoots().get(0).getValue(),
                // we don't need to check recursively for this special rewriting.
                EnumSet.of(LogicalOperatorTag.INNERJOIN, LogicalOperatorTag.LEFTOUTERJOIN))) {
            return new Pair<Boolean, ILogicalOperator>(false, null);
        }
        SubplanSpecialFlatteningCheckVisitor visitor = new SubplanSpecialFlatteningCheckVisitor();
        for (ILogicalPlan plan : subplanOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
                if (!opRef.getValue().accept(visitor, null)) {
                    return new Pair<Boolean, ILogicalOperator>(false, null);
                }
            }
        }
        return new Pair<Boolean, ILogicalOperator>(true, visitor.getQualifiedNts());
    }

}
