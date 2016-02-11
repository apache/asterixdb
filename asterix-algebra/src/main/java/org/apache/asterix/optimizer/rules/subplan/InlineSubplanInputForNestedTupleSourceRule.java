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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
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

import com.google.common.collect.ImmutableSet;

/*
This rule  is to remove SubplanOperators containing DataScan, InnerJoin, LeftOuterJoin, UnionAll or Distinct. Given a qualified Subplan operator called S1,
Let's call its input operator O1.

General Cases
We have the following rewritings for general cases:
R1. Replace all NestedTupleSourceOperators in S1 with deep-copies (with new variables) of the query plan rooted at O1;
R2. Add a LeftOuterOperatorJoinOperator (let's call it LJ) between O1 and the SubplanOperator's root operator's input (let's call it SO1),
    where O1 is the left branch and SO1 is the right branch;
R3. The deep copy of the primary key variables in O1 should be preserved from an inlined NestedTupleSourceOperator to SO1.
    The join condition of LJ is the equality between the primary key variables in O1 and its deep copied version at SO1;
R4. A variable v indicating non-match tuples is assigned to TRUE between LJ and SO1;
R5. On top of the LJ, add a GroupByOperaptor in which the nested plan consists of the S1's root operator, i.e., an aggregate operator.
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

In the plan, v_lc_1, ..., v_lc_n are live "covering" variables at InputOp, while v_rc_1, ..., v_rc_n are their corresponding variables populated from the deepcopy of InputOp.
"Covering" variables form a set of variables that can imply all live variables. v_l1, ....v_ln in the decoration part of the added group-by operator are all live variables
at InputOp except the covering variables v_lc_1, ..., v_lc_n.  In the current implementation, we use "covering" variables as primary key variables. In the next version, we
will use the real primary key variables, which will fix ASTERIXDB-1168.

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

Rewriting R2 is not necessary since before J1, all tuples from N1 are preserved. But the following rewritings are needed:
R1'. Replace N1 by the O1 (no additional deep copy);
R2'. All inner joins on the path from N1 to J1 (including J1) become left-outer joins with the same join conditions;
R3'. If N1 resides in the right branch of an inner join (let's call it J2) in the path from N1 to J1, switch the left and right branches of J2;
R4'. For every left join from N1 to J1 transformed from an inner join, a variable vi indicating non-match tuples is assigned to TRUE in its right branch;
R5'. On top of J1, a GroupByOperaptor G1 is added where the group-by key is the primary key of O1 and the nested query plan for aggregation is the nested pipeline
     on top of J1 with an added not-null-filter to check all vi are not null.
R6'. All other NestedTupleSourceOperators in the subplan is inlined with deep copies (with new variables) of the query plan rooted at O1.

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

In the plan, v_lc_1, ..., v_lc_n are live "covering" variables at InputOp and v_l1, ....v_ln in the decoration part of the added group-by operator are all live variables
at InputOp except the covering variables v_lc_1, ..., v_lc_n.  In the current implementation, we use "covering" variables as primary key variables. In the next version,
we will use the real primary key variables, which will fix ASTERIXDB-1168.

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
        Pair<Boolean, Map<LogicalVariable, LogicalVariable>> result = rewriteSubplanOperator(opRef, context);
        hasRun = true;
        return result.first;
    }

    private Pair<Boolean, Map<LogicalVariable, LogicalVariable>> rewriteSubplanOperator(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            // Traverses non-subplan operators.
            return traverseNonSubplanOperator(op, context);
        }
        /**
         * Apply the special join-based rewriting.
         */
        Pair<Boolean, Map<LogicalVariable, LogicalVariable>> result = applySpecialFlattening(opRef, context);
        if (!result.first) {
            /**
             * If the special join-based rewriting does not apply, apply the general
             * rewriting which blindly inlines all NTSs.
             */
            result = applyGeneralFlattening(opRef, context);
        }
        return result;
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
    private Pair<Boolean, Map<LogicalVariable, LogicalVariable>> traverseNonSubplanOperator(ILogicalOperator op,
            IOptimizationContext context) throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        Map<LogicalVariable, LogicalVariable> replacedVarMap = new HashMap<LogicalVariable, LogicalVariable>();
        Map<LogicalVariable, LogicalVariable> replacedVarMapForAncestor = new HashMap<LogicalVariable, LogicalVariable>();
        boolean changed = false;
        for (Mutable<ILogicalOperator> childrenRef : op.getInputs()) {
            Pair<Boolean, Map<LogicalVariable, LogicalVariable>> resultFromChild = rewriteSubplanOperator(childrenRef,
                    context);
            changed = changed || resultFromChild.first;
            for (Map.Entry<LogicalVariable, LogicalVariable> entry : resultFromChild.second.entrySet()) {
                if (liveVars.contains(entry.getKey())) {
                    // Only needs to map live variables for its ancestors.
                    replacedVarMapForAncestor.put(entry.getKey(), entry.getValue());
                }
            }
            replacedVarMap.putAll(resultFromChild.second);
        }
        VariableUtilities.substituteVariables(op, replacedVarMap, context);
        context.computeAndSetTypeEnvironmentForOperator(op);
        return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(changed, replacedVarMapForAncestor);
    }

    private Pair<Boolean, Map<LogicalVariable, LogicalVariable>> applyGeneralFlattening(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        SubplanOperator subplanOp = (SubplanOperator) opRef.getValue();
        if (!SubplanFlatteningUtil.containsOperators(subplanOp,
                ImmutableSet.of(LogicalOperatorTag.DATASOURCESCAN, LogicalOperatorTag.INNERJOIN,
                        // We don't have nested runtime for union-all and distinct hence we have to include them here.
                        LogicalOperatorTag.LEFTOUTERJOIN, LogicalOperatorTag.UNIONALL, LogicalOperatorTag.DISTINCT))) {
            // Traverses the operator as if it is not a subplan.
            return traverseNonSubplanOperator(subplanOp, context);
        }
        Mutable<ILogicalOperator> inputOpRef = subplanOp.getInputs().get(0);
        ILogicalOperator inputOp = inputOpRef.getValue();
        Pair<Map<LogicalVariable, LogicalVariable>, List<Pair<IOrder, Mutable<ILogicalExpression>>>> varMapAndOrderExprs = SubplanFlatteningUtil
                .inlineAllNestedTupleSource(subplanOp, context);
        Map<LogicalVariable, LogicalVariable> varMap = varMapAndOrderExprs.first;
        if (varMap == null) {
            // Traverses the operator as if it is not a subplan.
            return traverseNonSubplanOperator(subplanOp, context);
        }

        // Creates parameters for the left outer join operator.
        Set<LogicalVariable> inputLiveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(inputOp, inputLiveVars);
        Set<LogicalVariable> fdCoveringVars = EquivalenceClassUtils.findFDHeaderVariables(context, inputOp);

        Mutable<ILogicalOperator> rightInputOpRef = subplanOp.getNestedPlans().get(0).getRoots().get(0).getValue()
                .getInputs().get(0);
        ILogicalOperator rightInputOp = rightInputOpRef.getValue();

        // Creates a variable to indicate whether a left input tuple is killed in the plan rooted at rightInputOp.
        LogicalVariable assignVar = context.newVar();
        ILogicalOperator assignOp = new AssignOperator(assignVar,
                new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        assignOp.getInputs().add(rightInputOpRef);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        rightInputOpRef = new MutableObject<ILogicalOperator>(assignOp);

        // Constructs the join predicate for the leftOuter join.
        List<Mutable<ILogicalExpression>> joinPredicates = new ArrayList<Mutable<ILogicalExpression>>();
        for (LogicalVariable liveVar : fdCoveringVars) {
            List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
            arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar)));
            LogicalVariable rightVar = varMap.get(liveVar);
            arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(rightVar)));
            ILogicalExpression expr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.EQ), arguments);
            joinPredicates.add(new MutableObject<ILogicalExpression>(expr));
        }

        ILogicalExpression joinExpr = joinPredicates.size() > 1
                ? new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.AND),
                        joinPredicates)
                : joinPredicates.get(0).getValue();
        LeftOuterJoinOperator leftOuterJoinOp = new LeftOuterJoinOperator(
                new MutableObject<ILogicalExpression>(joinExpr), inputOpRef, rightInputOpRef);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rightInputOp, context);
        context.computeAndSetTypeEnvironmentForOperator(leftOuterJoinOp);

        // Creates group-by operator.
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByDecorList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        List<ILogicalPlan> nestedPlans = new ArrayList<ILogicalPlan>();
        GroupByOperator groupbyOp = new GroupByOperator(groupByList, groupByDecorList, nestedPlans);

        Map<LogicalVariable, LogicalVariable> replacedVarMap = new HashMap<>();
        for (LogicalVariable liveVar : fdCoveringVars) {
            LogicalVariable newVar = context.newVar();
            groupByList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(newVar,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar))));
            // Adds variables for replacements in ancestors.
            replacedVarMap.put(liveVar, newVar);
        }
        for (LogicalVariable liveVar : inputLiveVars) {
            if (fdCoveringVars.contains(liveVar)) {
                continue;
            }
            groupByDecorList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(null,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar))));
        }

        // Sets up the nested plan for the groupby operator.
        Mutable<ILogicalOperator> aggOpRef = subplanOp.getNestedPlans().get(0).getRoots().get(0);
        aggOpRef.getValue().getInputs().clear();

        Mutable<ILogicalOperator> currentOpRef = aggOpRef;
        // Adds an optional order operator.
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = varMapAndOrderExprs.second;
        if (!orderExprs.isEmpty()) {
            OrderOperator orderOp = new OrderOperator(orderExprs);
            currentOpRef = new MutableObject<ILogicalOperator>(orderOp);
            aggOpRef.getValue().getInputs().add(currentOpRef);
        }

        // Adds a select operator into the nested plan for group-by to remove tuples with NULL on {@code assignVar}, i.e.,
        // subplan input tuples that are filtered out within a subplan.
        Mutable<ILogicalExpression> filterVarExpr = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(assignVar));
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        args.add(filterVarExpr);
        List<Mutable<ILogicalExpression>> argsForNotFunction = new ArrayList<Mutable<ILogicalExpression>>();
        argsForNotFunction.add(new MutableObject<ILogicalExpression>(
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_NULL), args)));
        SelectOperator selectOp = new SelectOperator(
                new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.NOT), argsForNotFunction)),
                false, null);
        currentOpRef.getValue().getInputs().add(new MutableObject<ILogicalOperator>(selectOp));

        selectOp.getInputs().add(new MutableObject<ILogicalOperator>(
                new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(groupbyOp))));
        List<Mutable<ILogicalOperator>> nestedRoots = new ArrayList<Mutable<ILogicalOperator>>();
        nestedRoots.add(aggOpRef);
        nestedPlans.add(new ALogicalPlanImpl(nestedRoots));
        groupbyOp.getInputs().add(new MutableObject<ILogicalOperator>(leftOuterJoinOp));

        // Replaces subplan with the group-by operator.
        opRef.setValue(groupbyOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(groupbyOp, context);

        // Recursively applys this rule to the nested plan of the subplan operator,
        // for the case where there are nested subplan operators within {@code subplanOp}.
        Pair<Boolean, Map<LogicalVariable, LogicalVariable>> result = rewriteSubplanOperator(rightInputOpRef, context);
        VariableUtilities.substituteVariables(leftOuterJoinOp, result.second, context);
        VariableUtilities.substituteVariables(groupbyOp, result.second, context);

        // No var mapping from the right input operator should be populated up.
        return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(true, replacedVarMap);
    }

    private Pair<Boolean, Map<LogicalVariable, LogicalVariable>> applySpecialFlattening(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        SubplanOperator subplanOp = (SubplanOperator) opRef.getValue();
        ILogicalOperator inputOp = subplanOp.getInputs().get(0).getValue();
        Map<LogicalVariable, LogicalVariable> replacedVarMap = new HashMap<>();

        // Recursively applies this rule to the nested plan of the subplan operator,
        // for the case where there are nested subplan operators within {@code subplanOp}.
        Pair<Boolean, Map<LogicalVariable, LogicalVariable>> result = rewriteSubplanOperator(
                subplanOp.getNestedPlans().get(0).getRoots().get(0), context);

        Pair<Set<LogicalVariable>, Mutable<ILogicalOperator>> notNullVarsAndTopJoinRef = SubplanFlatteningUtil
                .inlineLeftNtsInSubplanJoin(subplanOp, context);
        if (notNullVarsAndTopJoinRef.first == null) {
            return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(false, replacedVarMap);
        }

        Set<LogicalVariable> notNullVars = notNullVarsAndTopJoinRef.first;
        Mutable<ILogicalOperator> topJoinRef = notNullVarsAndTopJoinRef.second;

        // Gets live variables and covering variables from the subplan's input operator.
        Set<LogicalVariable> fdCoveringVars = EquivalenceClassUtils.findFDHeaderVariables(context, inputOp);
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(inputOp, liveVars);

        // Creates a group-by operator.
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByDecorList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        GroupByOperator groupbyOp = new GroupByOperator(groupByList, groupByDecorList, subplanOp.getNestedPlans());

        Map<LogicalVariable, LogicalVariable> gbyVarMap = new HashMap<LogicalVariable, LogicalVariable>();
        for (LogicalVariable coverVar : fdCoveringVars) {
            LogicalVariable newVar = context.newVar();
            gbyVarMap.put(coverVar, newVar);
            groupByList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(newVar,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(coverVar))));
            // Adds variables for replacements in ancestors.
            replacedVarMap.put(coverVar, newVar);
        }
        for (LogicalVariable liveVar : liveVars) {
            if (fdCoveringVars.contains(liveVar)) {
                continue;
            }
            groupByDecorList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(null,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar))));
        }
        groupbyOp.getInputs().add(new MutableObject<ILogicalOperator>(topJoinRef.getValue()));

        // Adds a select operator into the nested plan for group-by to remove tuples with NULL on {@code assignVar}, i.e.,
        // subplan input tuples that are filtered out within a subplan.
        List<Mutable<ILogicalExpression>> nullCheckExprRefs = new ArrayList<>();
        for (LogicalVariable notNullVar : notNullVars) {
            Mutable<ILogicalExpression> filterVarExpr = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(notNullVar));
            List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
            args.add(filterVarExpr);
            List<Mutable<ILogicalExpression>> argsForNotFunction = new ArrayList<Mutable<ILogicalExpression>>();
            argsForNotFunction.add(new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_NULL), args)));
            nullCheckExprRefs.add(new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.NOT), argsForNotFunction)));
        }
        Mutable<ILogicalExpression> selectExprRef = nullCheckExprRefs.size() > 1
                ? new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.AND), nullCheckExprRefs))
                : nullCheckExprRefs.get(0);
        SelectOperator selectOp = new SelectOperator(selectExprRef, false, null);
        topJoinRef.setValue(selectOp);
        selectOp.getInputs().add(new MutableObject<ILogicalOperator>(
                new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(groupbyOp))));

        opRef.setValue(groupbyOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(groupbyOp, context);

        VariableUtilities.substituteVariables(groupbyOp, result.second, context);
        replacedVarMap.putAll(result.second);
        return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(true, replacedVarMap);
    }
}
