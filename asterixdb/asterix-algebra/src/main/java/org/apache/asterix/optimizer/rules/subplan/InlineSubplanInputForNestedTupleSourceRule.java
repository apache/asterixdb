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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/*
This rule  is to remove SubplanOperators containing DataScan, InnerJoin, LeftOuterJoin.
Given a qualified Subplan operator called S1, Let's call its input operator O1.

General Cases
We have the following rewritings for general cases:
R1. Replace all NestedTupleSourceOperators in S1 with deep-copies (with new variables) of the query plan rooted at O1;
R2. Add a LeftOuterOperatorJoinOperator (let's call it LJ) between O1 and the SubplanOperator's root operator's input
    (let's call it SO1),where O1 is the left branch and SO1 is the right branch;
R3. The deep copy of the primary key variables in O1 should be preserved from an inlined NestedTupleSourceOperator
    to SO1. The join condition of LJ is the equality between the primary key variables in O1 and its deep copied
    version at SO1;
R4. A variable v indicating non-match tuples is assigned to TRUE between LJ and SO1;
R5. On top of the LJ, add a GroupByOperaptor in which the nested plan consists of the S1's root operator,
    i.e., an aggregate operator.
    Below the aggregate, there is a not-null-filter on variable v. The group key is the primary key variables in O1.

This is an abstract example for the rewriting mechanism described above:
Before rewriting:
--Op1
  --Subplan{
    --AggregateOp
      --NestedOp
        .....
          --Nested-Tuple-Source
    }
    --InputOp
      .....

After rewriting:
--Op1
  --GroupBy v_lc_1, ..., v_lc_n Decor v_l1, ....v_ln {
            --AggregateOp
              --Select v_new!=NULL
                -- Nested-Tuple-Source
          }
     --LeftOuterJoin (v_lc_1=v_rc_1 AND .... AND v_lc_n=v_rc_n)
       (left branch)
         --InputOp
            .....
       (right branch)
         -- Assign v_new=TRUE
           --NestedOp
             .....
               --Deepcopy_The_Plan_Rooted_At_InputOp_With_New_Variables(InputOp)

In the plan, v_lc_1, ..., v_lc_n are live "covering" variables at InputOp, while v_rc_1, ..., v_rc_n are their
corresponding variables populated from the deepcopy of InputOp.
"Covering" variables form a set of variables that can imply all live variables. v_l1, ....v_ln in the decoration part
of the added group-by operator are all live variables at InputOp except the covering variables v_lc_1, ..., v_lc_n.
In the current implementation, we use "covering" variables as primary key variables.
In the next version, we will use the real primary key variables, which will fix ASTERIXDB-1168.

Here is a concrete example of the general case rewriting (optimizerts/queries/nested_loj4.aql).
Before plan:
distribute result [%0->$$13] -- |UNPARTITIONED|
  project ([$$13]) -- |UNPARTITIONED|
    assign [$$13] <- [function-call: asterix:open-record-constructor, Args:[AString: {cust}, %0->$$0, AString: {orders}, %0->$$12]] -- |UNPARTITIONED|
      subplan {
                aggregate [$$12] <- [function-call: asterix:listify, Args:[%0->$$1]] -- |UNPARTITIONED|
                  join (function-call: algebricks:eq, Args:[%0->$$16, %0->$$14]) -- |UNPARTITIONED|
                    select (function-call: algebricks:eq, Args:[%0->$$18, AInt64: {5}]) -- |UNPARTITIONED|
                      nested tuple source -- |UNPARTITIONED|
                    assign [$$16] <- [function-call: asterix:field-access-by-name, Args:[%0->$$19, AString: {o_custkey}]] -- |UNPARTITIONED|
                      assign [$$19] <- [function-call: asterix:field-access-by-name, Args:[%0->$$1, AString: {o_$o}]] -- |UNPARTITIONED|
                        data-scan []<-[$$15, $$1] <- tpch:Orders -- |UNPARTITIONED|
                          empty-tuple-source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        assign [$$18] <- [function-call: asterix:field-access-by-index, Args:[%0->$$0, AInt32: {3}]] -- |UNPARTITIONED|
          data-scan []<-[$$14, $$0] <- tpch:Customers -- |UNPARTITIONED|
            empty-tuple-source -- |UNPARTITIONED|

 After plan:
distribute result [%0->$$13] -- |UNPARTITIONED|
  project ([$$13]) -- |UNPARTITIONED|
    assign [$$13] <- [function-call: asterix:open-record-constructor, Args:[AString: {cust}, %0->$$0, AString: {orders}, %0->$$12]] -- |UNPARTITIONED|
      group by ([$$24 := %0->$$14]) decor ([%0->$$0; %0->$$18]) {
                aggregate [$$12] <- [function-call: asterix:listify, Args:[%0->$$1]] -- |UNPARTITIONED|
                  select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$23]]) -- |UNPARTITIONED|
                    nested tuple source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        left outer join (function-call: algebricks:eq, Args:[%0->$$14, %0->$$22]) -- |UNPARTITIONED|
          assign [$$18] <- [function-call: asterix:field-access-by-index, Args:[%0->$$0, AInt32: {3}]] -- |UNPARTITIONED|
            data-scan []<-[$$14, $$0] <- tpch:Customers -- |UNPARTITIONED|
              empty-tuple-source -- |UNPARTITIONED|
          assign [$$23] <- [TRUE] -- |UNPARTITIONED|
            join (function-call: algebricks:eq, Args:[%0->$$16, %0->$$22]) -- |UNPARTITIONED|
              select (function-call: algebricks:eq, Args:[%0->$$20, AInt64: {5}]) -- |UNPARTITIONED|
                assign [$$20] <- [function-call: asterix:field-access-by-index, Args:[%0->$$21, AInt32: {3}]] -- |UNPARTITIONED|
                  data-scan []<-[$$22, $$21] <- tpch:Customers -- |UNPARTITIONED|
                    empty-tuple-source -- |UNPARTITIONED|
              assign [$$16] <- [function-call: asterix:field-access-by-name, Args:[%0->$$19, AString: {o_custkey}]] -- |UNPARTITIONED|
                assign [$$19] <- [function-call: asterix:field-access-by-name, Args:[%0->$$1, AString: {o_$o}]] -- |UNPARTITIONED|
                  data-scan []<-[$$15, $$1] <- tpch:Orders -- |UNPARTITIONED|
                    empty-tuple-source -- |UNPARTITIONED|

Special Cases
For special cases where:
a. there is a join (let's call it J1.) in the nested plan,
b. if J1 is an inner join, one input pipeline of J1 has a NestedTupleSource descendant (let's call it N1),
c. if J1 is a left outer join, the left branch of J1 has a NestedTupleSource descendant (let's call it N1),
d. there is no tuple dropping from N1 to J1

Rewriting R2 is not necessary since before J1, all tuples from N1 are preserved. But the following rewritings are
needed:
R1'. Replace N1 by the O1 (no additional deep copy);
R2'. All inner joins on the path from N1 to J1 (including J1) become left-outer joins with the same join conditions;
R3'. If N1 resides in the right branch of an inner join (let's call it J2) in the path from N1 to J1,
     switch the left and right branches of J2;
R4'. For every left join from N1 to J1 transformed from an inner join, a variable vi indicating non-match tuples
     is assigned to TRUE in its right branch;
R5'. On top of J1, a GroupByOperaptor G1 is added where the group-by key is the primary key of O1 and
     the nested query plan for aggregation is the nested pipeline on top of J1 with an added not-null-filter
     to check all vi are not null.
R6'. All other NestedTupleSourceOperators in the subplan is inlined with deep copies (with new variables)
     of the query plan rooted at O1.

This is an abstract example for the special rewriting mechanism described above:
Before rewriting:
--Op1
  --Subplan{
    --AggregateOp
      --NestedOp
        – Inner Join (J1)
          – (Right branch) ..... (L1)
          – (Left branch) ..... (R1)
                    --Nested-Tuple-Source
    }
    --InputOp
      .....
(Note that pipeline R1 must satisfy the condition that it does not drop any tuples.)
After rewriting:
-- Op1
  – GroupBy v_lc_1, ..., v_lc_n Decor v_l1, ....v_ln {
            – AggregateOp
               – NestedOp
                 – Select v_new!=NULL
                   – Nested-Tuple-Source
          }
     --LeftOuterJoin (J1)
       (left branch)
              –  ......  (R1)
                 – InputOp
                   .....
       (right branch)
             – Assign v_new=TRUE
                – ..... (L1)

In the plan, v_lc_1, ..., v_lc_n are live "covering" variables at InputOp and v_l1, ....v_ln in the decoration part
of the added group-by operator are all live variables at InputOp except the covering variables v_lc_1, ..., v_lc_n.
In the current implementation, we use "covering" variables as primary key variables.
In the next version, we will use the real primary key variables, which will fix ASTERIXDB-1168.

Here is a concrete example (optimizerts/queries/nested_loj2.aql). .
Before plan:
distribute result [%0->$$17] -- |UNPARTITIONED|
  project ([$$17]) -- |UNPARTITIONED|
    assign [$$17] <- [function-call: asterix:open-record-constructor, Args:[AString: {cust}, %0->$$0, AString: {orders}, %0->$$16]] -- |UNPARTITIONED|
      subplan {
                aggregate [$$16] <- [function-call: asterix:listify, Args:[%0->$$15]] -- |UNPARTITIONED|
                  assign [$$15] <- [function-call: asterix:open-record-constructor, Args:[AString: {order}, %0->$$1, AString: {items}, %0->$$14]] -- |UNPARTITIONED|
                    subplan {
                              aggregate [$$14] <- [function-call: asterix:listify, Args:[%0->$$2]] -- |UNPARTITIONED|
                                join (function-call: algebricks:eq, Args:[%0->$$20, %0->$$19]) -- |UNPARTITIONED|
                                  nested tuple source -- |UNPARTITIONED|
                                  data-scan []<-[$$20, $$21, $$2] <- tpch:LineItems -- |UNPARTITIONED|
                                    empty-tuple-source -- |UNPARTITIONED|
                           } -- |UNPARTITIONED|
                      join (function-call: algebricks:eq, Args:[%0->$$22, %0->$$18]) -- |UNPARTITIONED|
                        nested tuple source -- |UNPARTITIONED|
                        assign [$$22] <- [function-call: asterix:field-access-by-index, Args:[%0->$$1, AInt32: {1}]] -- |UNPARTITIONED|
                          data-scan []<-[$$19, $$1] <- tpch:Orders -- |UNPARTITIONED|
                            empty-tuple-source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        data-scan []<-[$$18, $$0] <- tpch:Customers -- |UNPARTITIONED|
          empty-tuple-source -- |UNPARTITIONED|

After plan:
distribute result [%0->$$17] -- |UNPARTITIONED|
  project ([$$17]) -- |UNPARTITIONED|
    assign [$$17] <- [function-call: asterix:open-record-constructor, Args:[AString: {cust}, %0->$$0, AString: {orders}, %0->$$16]] -- |UNPARTITIONED|
      group by ([$$30 := %0->$$18]) decor ([%0->$$0]) {
                aggregate [$$16] <- [function-call: asterix:listify, Args:[%0->$$15]] -- |UNPARTITIONED|
                  assign [$$15] <- [function-call: asterix:open-record-constructor, Args:[AString: {order}, %0->$$1, AString: {items}, %0->$$14]] -- |UNPARTITIONED|
                    group by ([$$27 := %0->$$19]) decor ([%0->$$0; %0->$$1; %0->$$18; %0->$$22]) {
                              aggregate [$$14] <- [function-call: asterix:listify, Args:[%0->$$2]] -- |UNPARTITIONED|
                                select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$26]]) -- |UNPARTITIONED|
                                  nested tuple source -- |UNPARTITIONED|
                           } -- |UNPARTITIONED|
                      select (function-call: algebricks:and, Args:[function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$28]], function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$29]]]) -- |UNPARTITIONED|
                        nested tuple source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        left outer join (function-call: algebricks:eq, Args:[%0->$$20, %0->$$19]) -- |UNPARTITIONED|
          left outer join (function-call: algebricks:eq, Args:[%0->$$22, %0->$$18]) -- |UNPARTITIONED|
            data-scan []<-[$$18, $$0] <- tpch:Customers -- |UNPARTITIONED|
              empty-tuple-source -- |UNPARTITIONED|
            assign [$$28] <- [TRUE] -- |UNPARTITIONED|
              assign [$$22] <- [function-call: asterix:field-access-by-index, Args:[%0->$$1, AInt32: {1}]] -- |UNPARTITIONED|
                data-scan []<-[$$19, $$1] <- tpch:Orders -- |UNPARTITIONED|
                  empty-tuple-source -- |UNPARTITIONED|
          assign [$$29] <- [TRUE] -- |UNPARTITIONED|
            assign [$$26] <- [TRUE] -- |UNPARTITIONED|
              data-scan []<-[$$20, $$21, $$2] <- tpch:LineItems -- |UNPARTITIONED|
                empty-tuple-source -- |UNPARTITIONED|
*/
public class InlineSubplanInputForNestedTupleSourceRule implements IAlgebraicRewriteRule {

    // To make sure the rule only runs once.
    private boolean hasRun = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (hasRun) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> result = rewriteSubplanOperator(opRef, context);
        hasRun = true;
        return result.first;
    }

    private Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> rewriteSubplanOperator(
            Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // Recursively traverses input operators as if the current operator before rewriting the current operator.
        Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> changedAndVarMap =
                traverseNonSubplanOperator(op, context);
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return changedAndVarMap;
        }

        /**
         * Apply the special join-based rewriting.
         */
        Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> result = applySpecialFlattening(opRef, context);
        if (!result.first) {
            /**
             * If the special join-based rewriting does not apply, apply the general
             * rewriting which blindly inlines all NTSs.
             */
            result = applyGeneralFlattening(opRef, context);
        }
        LinkedHashMap<LogicalVariable, LogicalVariable> returnedMap = new LinkedHashMap<>();
        // Adds variable mappings from input operators.
        returnedMap.putAll(changedAndVarMap.second);
        // Adds variable mappings resulting from the rewriting of the current operator.
        returnedMap.putAll(result.second);
        return new Pair<>(result.first || changedAndVarMap.first, returnedMap);
    }

    /***
     * Deals with operators that are not SubplanOperator.
     *
     * @param op
     *            the operator to consider
     * @param context
     * @return
     * @throws AlgebricksException
     */
    private Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> traverseNonSubplanOperator(
            ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        LinkedHashMap<LogicalVariable, LogicalVariable> replacedVarMap = new LinkedHashMap<>();
        LinkedHashMap<LogicalVariable, LogicalVariable> replacedVarMapForAncestor = new LinkedHashMap<>();
        boolean changed = false;
        for (Mutable<ILogicalOperator> childrenRef : op.getInputs()) {
            Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> resultFromChild =
                    rewriteSubplanOperator(childrenRef, context);
            changed = changed || resultFromChild.first;
            resultFromChild.second.forEach((oldVar, newVar) -> {
                if (liveVars.contains(oldVar)) {
                    // Maps live variables for its ancestors.
                    replacedVarMapForAncestor.put(oldVar, newVar);
                    // Recursively maps live variables for its ancestors.
                    oldVar = newVar;
                    while ((newVar = resultFromChild.second.get(newVar)) != null) {
                        replacedVarMapForAncestor.put(oldVar, newVar);
                        oldVar = newVar;
                    }
                }
            });
            replacedVarMap.putAll(resultFromChild.second);
        }
        VariableUtilities.substituteVariables(op, replacedVarMap, context);
        context.computeAndSetTypeEnvironmentForOperator(op);
        return new Pair<>(changed, replacedVarMapForAncestor);
    }

    private Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> applyGeneralFlattening(
            Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        SubplanOperator subplanOp = (SubplanOperator) opRef.getValue();
        if (!SubplanFlatteningUtil.containsOperators(subplanOp, EnumSet.of(LogicalOperatorTag.DATASOURCESCAN,
                LogicalOperatorTag.INNERJOIN, LogicalOperatorTag.LEFTOUTERJOIN))) {
            return new Pair<>(false, new LinkedHashMap<>());
        }
        SourceLocation sourceLoc = subplanOp.getSourceLocation();
        Mutable<ILogicalOperator> inputOpRef = subplanOp.getInputs().get(0);
        ILogicalOperator inputOpBackup = inputOpRef.getValue();
        // Creates parameters for the left outer join operator.
        Pair<ILogicalOperator, Set<LogicalVariable>> primaryOpAndVars =
                EquivalenceClassUtils.findOrCreatePrimaryKeyOpAndVariables(inputOpBackup, true, context);
        ILogicalOperator inputOp = primaryOpAndVars.first;
        Set<LogicalVariable> primaryKeyVars = primaryOpAndVars.second;
        inputOpRef.setValue(inputOp);
        Set<LogicalVariable> inputLiveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(inputOp, inputLiveVars);

        Pair<Map<LogicalVariable, LogicalVariable>, List<Pair<IOrder, Mutable<ILogicalExpression>>>> varMapAndOrderExprs =
                SubplanFlatteningUtil.inlineAllNestedTupleSource(subplanOp, context);
        Map<LogicalVariable, LogicalVariable> varMap = varMapAndOrderExprs.first;
        if (varMap == null) {
            inputOpRef.setValue(inputOpBackup);
            return new Pair<>(false, new LinkedHashMap<>());
        }

        Mutable<ILogicalOperator> lowestAggregateRefInSubplan =
                SubplanFlatteningUtil.findLowestAggregate(subplanOp.getNestedPlans().get(0).getRoots().get(0));
        Mutable<ILogicalOperator> rightInputOpRef = lowestAggregateRefInSubplan.getValue().getInputs().get(0);
        ILogicalOperator rightInputOp = rightInputOpRef.getValue();

        // Creates a variable to indicate whether a left input tuple is killed in the plan rooted at rightInputOp.
        LogicalVariable assignVar = context.newVar();
        AssignOperator assignOp = new AssignOperator(assignVar, new MutableObject<>(ConstantExpression.TRUE));
        assignOp.setSourceLocation(rightInputOp.getSourceLocation());
        assignOp.getInputs().add(rightInputOpRef);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        rightInputOpRef = new MutableObject<>(assignOp);

        // Constructs the join predicate for the leftOuter join.
        List<Mutable<ILogicalExpression>> joinPredicates = new ArrayList<>();
        for (LogicalVariable liveVar : primaryKeyVars) {
            List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
            VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
            liveVarRef.setSourceLocation(sourceLoc);
            arguments.add(new MutableObject<>(liveVarRef));
            LogicalVariable rightVar = varMap.get(liveVar);
            VariableReferenceExpression rightVarRef = new VariableReferenceExpression(rightVar);
            rightVarRef.setSourceLocation(sourceLoc);
            arguments.add(new MutableObject<>(rightVarRef));
            ScalarFunctionCallExpression expr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.EQ), arguments);
            expr.setSourceLocation(sourceLoc);
            joinPredicates.add(new MutableObject<>(expr));
        }

        ILogicalExpression joinExpr;
        if (joinPredicates.size() > 1) {
            ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.AND), joinPredicates);
            andExpr.setSourceLocation(sourceLoc);
            joinExpr = andExpr;
        } else {
            joinExpr = joinPredicates.size() > 0 ? joinPredicates.get(0).getValue() : ConstantExpression.TRUE;
        }
        LeftOuterJoinOperator leftOuterJoinOp =
                new LeftOuterJoinOperator(new MutableObject<>(joinExpr), inputOpRef, rightInputOpRef);
        leftOuterJoinOp.setSourceLocation(sourceLoc);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rightInputOp, context);
        context.computeAndSetTypeEnvironmentForOperator(leftOuterJoinOp);

        // Creates group-by operator.
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByDecorList = new ArrayList<>();
        List<ILogicalPlan> nestedPlans = new ArrayList<>();
        GroupByOperator groupbyOp = new GroupByOperator(groupByList, groupByDecorList, nestedPlans);
        groupbyOp.setSourceLocation(sourceLoc);

        LinkedHashMap<LogicalVariable, LogicalVariable> replacedVarMap = new LinkedHashMap<>();
        for (LogicalVariable liveVar : primaryKeyVars) {
            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
            liveVarRef.setSourceLocation(inputOpBackup.getSourceLocation());
            groupByList.add(new Pair<>(newVar, new MutableObject<>(liveVarRef)));
            // Adds variables for replacements in ancestors.
            replacedVarMap.put(liveVar, newVar);
        }
        for (LogicalVariable liveVar : inputLiveVars) {
            if (primaryKeyVars.contains(liveVar)) {
                continue;
            }
            VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
            liveVarRef.setSourceLocation(sourceLoc);
            groupByDecorList.add(new Pair<>(null, new MutableObject<>(liveVarRef)));
        }

        // Sets up the nested plan for the groupby operator.
        Mutable<ILogicalOperator> aggOpRef = subplanOp.getNestedPlans().get(0).getRoots().get(0);
        lowestAggregateRefInSubplan.getValue().getInputs().clear(); // Clears the input of the lowest aggregate.
        Mutable<ILogicalOperator> currentOpRef = lowestAggregateRefInSubplan;
        // Adds an optional order operator.
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = varMapAndOrderExprs.second;
        if (!orderExprs.isEmpty()) {
            OrderOperator orderOp = new OrderOperator(orderExprs);
            orderOp.setSourceLocation(sourceLoc);
            currentOpRef = new MutableObject<>(orderOp);
            lowestAggregateRefInSubplan.getValue().getInputs().add(currentOpRef);
        }

        // Adds a select operator into the nested plan for group-by to remove tuples with NULL on {@code assignVar},
        // i.e., subplan input tuples that are filtered out within a subplan.
        VariableReferenceExpression assignVarRef = new VariableReferenceExpression(assignVar);
        assignVarRef.setSourceLocation(sourceLoc);
        Mutable<ILogicalExpression> filterVarExpr = new MutableObject<>(assignVarRef);
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();
        args.add(filterVarExpr);
        List<Mutable<ILogicalExpression>> argsForNotFunction = new ArrayList<>();
        ScalarFunctionCallExpression isMissingExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.IS_MISSING), args);
        isMissingExpr.setSourceLocation(sourceLoc);
        argsForNotFunction.add(new MutableObject<>(isMissingExpr));
        ScalarFunctionCallExpression notExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.NOT), argsForNotFunction);
        notExpr.setSourceLocation(sourceLoc);
        SelectOperator selectOp = new SelectOperator(new MutableObject<>(notExpr), false, null);
        selectOp.setSourceLocation(sourceLoc);
        currentOpRef.getValue().getInputs().add(new MutableObject<>(selectOp));

        NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(groupbyOp));
        ntsOp.setSourceLocation(sourceLoc);
        selectOp.getInputs().add(new MutableObject<>(ntsOp));
        List<Mutable<ILogicalOperator>> nestedRoots = new ArrayList<>();
        nestedRoots.add(aggOpRef);
        nestedPlans.add(new ALogicalPlanImpl(nestedRoots));
        groupbyOp.getInputs().add(new MutableObject<>(leftOuterJoinOp));

        // Replaces subplan with the group-by operator.
        opRef.setValue(groupbyOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(groupbyOp, context);

        // Recursively applys this rule to the nested plan of the subplan operator,
        // for the case where there are nested subplan operators within {@code subplanOp}.
        Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> result =
                rewriteSubplanOperator(rightInputOpRef, context);
        VariableUtilities.substituteVariables(leftOuterJoinOp, result.second, context);
        VariableUtilities.substituteVariables(groupbyOp, result.second, context);

        // No var mapping from the right input operator should be populated up.
        return new Pair<>(true, replacedVarMap);
    }

    private Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> applySpecialFlattening(
            Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        SubplanOperator subplanOp = (SubplanOperator) opRef.getValue();
        SourceLocation sourceLoc = subplanOp.getSourceLocation();
        Mutable<ILogicalOperator> inputOpRef = subplanOp.getInputs().get(0);
        LinkedHashMap<LogicalVariable, LogicalVariable> replacedVarMap = new LinkedHashMap<>();

        // Recursively applies this rule to the nested plan of the subplan operator,
        // for the case where there are nested subplan operators within {@code subplanOp}.
        Pair<Boolean, LinkedHashMap<LogicalVariable, LogicalVariable>> result =
                rewriteSubplanOperator(subplanOp.getNestedPlans().get(0).getRoots().get(0), context);

        ILogicalOperator inputOpBackup = inputOpRef.getValue();
        // Gets live variables and covering variables from the subplan's input operator.
        Pair<ILogicalOperator, Set<LogicalVariable>> primaryOpAndVars =
                EquivalenceClassUtils.findOrCreatePrimaryKeyOpAndVariables(inputOpBackup, false, context);
        ILogicalOperator inputOp = primaryOpAndVars.first;
        Set<LogicalVariable> primaryKeyVars = primaryOpAndVars.second;
        inputOpRef.setValue(inputOp);
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(inputOp, liveVars);

        Pair<Set<LogicalVariable>, Mutable<ILogicalOperator>> notNullVarsAndTopJoinRef =
                SubplanFlatteningUtil.inlineLeftNtsInSubplanJoin(subplanOp, context);
        if (notNullVarsAndTopJoinRef.first == null) {
            inputOpRef.setValue(inputOpBackup);
            return new Pair<>(false, replacedVarMap);
        }

        Set<LogicalVariable> notNullVars = notNullVarsAndTopJoinRef.first;
        Mutable<ILogicalOperator> topJoinRef = notNullVarsAndTopJoinRef.second;

        // Creates a group-by operator.
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByDecorList = new ArrayList<>();
        GroupByOperator groupbyOp = new GroupByOperator(groupByList, groupByDecorList, subplanOp.getNestedPlans());
        groupbyOp.setSourceLocation(sourceLoc);

        for (LogicalVariable coverVar : primaryKeyVars) {
            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression coverVarRef = new VariableReferenceExpression(coverVar);
            coverVarRef.setSourceLocation(sourceLoc);
            groupByList.add(new Pair<>(newVar, new MutableObject<>(coverVarRef)));
            // Adds variables for replacements in ancestors.
            replacedVarMap.put(coverVar, newVar);
        }
        for (LogicalVariable liveVar : liveVars) {
            if (primaryKeyVars.contains(liveVar)) {
                continue;
            }
            VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
            liveVarRef.setSourceLocation(sourceLoc);
            groupByDecorList.add(new Pair<>(null, new MutableObject<>(liveVarRef)));
        }
        groupbyOp.getInputs().add(new MutableObject<>(topJoinRef.getValue()));

        if (!notNullVars.isEmpty()) {
            // Adds a select operator into the nested plan for group-by to remove tuples with NULL on {@code assignVar},
            // i.e., subplan input tuples that are filtered out within a subplan.
            List<Mutable<ILogicalExpression>> nullCheckExprRefs = new ArrayList<>();
            for (LogicalVariable notNullVar : notNullVars) {
                VariableReferenceExpression notNullVarRef = new VariableReferenceExpression(notNullVar);
                notNullVarRef.setSourceLocation(sourceLoc);
                Mutable<ILogicalExpression> filterVarExpr = new MutableObject<>(notNullVarRef);
                List<Mutable<ILogicalExpression>> args = new ArrayList<>();
                args.add(filterVarExpr);
                List<Mutable<ILogicalExpression>> argsForNotFunction = new ArrayList<>();
                ScalarFunctionCallExpression isMissingExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.IS_MISSING), args);
                isMissingExpr.setSourceLocation(sourceLoc);
                argsForNotFunction.add(new MutableObject<>(isMissingExpr));
                ScalarFunctionCallExpression notExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.NOT), argsForNotFunction);
                notExpr.setSourceLocation(sourceLoc);
                nullCheckExprRefs.add(new MutableObject<>(notExpr));
            }
            Mutable<ILogicalExpression> selectExprRef;
            if (nullCheckExprRefs.size() > 1) {
                ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.AND), nullCheckExprRefs);
                andExpr.setSourceLocation(sourceLoc);
                selectExprRef = new MutableObject<>(andExpr);
            } else {
                selectExprRef = nullCheckExprRefs.get(0);
            }
            SelectOperator selectOp = new SelectOperator(selectExprRef, false, null);
            selectOp.setSourceLocation(sourceLoc);
            topJoinRef.setValue(selectOp);
            NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(groupbyOp));
            ntsOp.setSourceLocation(sourceLoc);
            selectOp.getInputs().add(new MutableObject<>(ntsOp));
        } else {
            // The original join operator in the Subplan is a left-outer join.
            // Therefore, no null-check variable is injected and no SelectOperator needs to be added.
            NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(groupbyOp));
            ntsOp.setSourceLocation(sourceLoc);
            topJoinRef.setValue(ntsOp);
        }
        opRef.setValue(groupbyOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(groupbyOp, context);

        VariableUtilities.substituteVariables(groupbyOp, result.second, context);
        replacedVarMap.putAll(result.second);
        return new Pair<>(true, replacedVarMap);
    }
}
