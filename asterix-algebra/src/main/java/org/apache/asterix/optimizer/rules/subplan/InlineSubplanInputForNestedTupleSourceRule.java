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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;

/***
 * This rule inlines (deep copies) a SubplanOperator's input query plan to
 * replace its NestedTupleSources.
 * Then, the SubplanOperator is replaced by:
 * 1. a LeftOuterOperatorJoin between the SubplanOperator's input operator and the
 * SubplanOperator's root operator's input.
 * 2. and on top of the LeftOuterJoinOperator, a GroupByOperaptor in which the
 * nested plan consists of the SubplanOperator's root operator.
 */
/*
This is an abstract example for this rule:

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

In the plan, v_lc_1, ..., v_lc_n are live "covering" variables at InputOp,
while v_rc_1, ..., v_rc_n are their corresponding variables populated from the deepcopy of InputOp.
("Covering" variables form a set of variables that can imply all live variables.)
v_l1, ....v_ln in the decoration part of the added group-by operator are all
live variables at InputOp except the covering variables v_lc_1, ..., v_lc_n.

TODO(buyingyi): the rewritten plan is wrong when there are duplicate tuples from InputOp: ASTERIXDB-1168.

Here are two concrete examples. (The top child of a join operator is the outer branch.)
---------- Example 1 -----------
FINE: >>>> Before plan
distribute result [%0->$$27] -- |UNPARTITIONED|
  project ([$$27]) -- |UNPARTITIONED|
    assign [$$27] <- [function-call: asterix:open-record-constructor, Args:[AString: {subscription-id}, %0->$$37, AString: {execution-time}, function-call: asterix:current-datetime, Args:[], AString: {result}, %0->$$6]] -- |UNPARTITIONED|
      unnest $$6 <- function-call: asterix:scan-collection, Args:[%0->$$26] -- |UNPARTITIONED|
        subplan {
                  aggregate [$$26] <- [function-call: asterix:listify, Args:[%0->$$22]] -- |UNPARTITIONED|
                    join (TRUE) -- |UNPARTITIONED|
                      select (%0->$$21) -- |UNPARTITIONED|
                        group by ([$$30 := %0->$$35]) decor ([%0->$$5; %0->$$7; %0->$$8; %0->$$31]) {
                                  aggregate [$$21] <- [function-call: asterix:non-empty-stream, Args:[]] -- |UNPARTITIONED|
                                    select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$34]]) -- |UNPARTITIONED|
                                      nested tuple source -- |UNPARTITIONED|
                               } -- |UNPARTITIONED|
                          left outer join (function-call: algebricks:eq, Args:[%0->$$36, %0->$$7]) -- |UNPARTITIONED|
                            data-scan []<-[$$31, $$8] <- emergencyTest:CHPReports -- |UNPARTITIONED|
                              nested tuple source -- |UNPARTITIONED|
                            assign [$$34] <- [TRUE] -- |UNPARTITIONED|
                              assign [$$36] <- [function-call: asterix:field-access-by-index, Args:[%0->$$10, AInt32: {1}]] -- |UNPARTITIONED|
                                data-scan []<-[$$32, $$10] <- emergencyTest:userLocations -- |UNPARTITIONED|
                                  empty-tuple-source -- |UNPARTITIONED|
                      assign [$$22] <- [function-call: asterix:open-record-constructor, Args:[AString: {shelter locations}, %0->$$25]] -- |UNPARTITIONED|
                        aggregate [$$25] <- [function-call: asterix:listify, Args:[%0->$$24]] -- |UNPARTITIONED|
                          assign [$$24] <- [function-call: asterix:field-access-by-index, Args:[%0->$$11, AInt32: {1}]] -- |UNPARTITIONED|
                            data-scan []<-[$$33, $$11] <- emergencyTest:tornadoShelters -- |UNPARTITIONED|
                              empty-tuple-source -- |UNPARTITIONED|
               } -- |UNPARTITIONED|
          assign [$$7] <- [function-call: asterix:field-access-by-index, Args:[%0->$$5, AInt32: {1}]] -- |UNPARTITIONED|
            assign [$$37] <- [function-call: asterix:field-access-by-name, Args:[%0->$$5, AString: {subscription-id}]] -- |UNPARTITIONED|
              data-scan []<-[$$35, $$5] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                empty-tuple-source -- |UNPARTITIONED|


Dec 22, 2015 4:39:22 PM org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController printRuleApplication
FINE: >>>> After plan
distribute result [%0->$$27] -- |UNPARTITIONED|
  project ([$$27]) -- |UNPARTITIONED|
    assign [$$27] <- [function-call: asterix:open-record-constructor, Args:[AString: {subscription-id}, %0->$$37, AString: {execution-time}, function-call: asterix:current-datetime, Args:[], AString: {result}, %0->$$6]] -- |UNPARTITIONED|
      unnest $$6 <- function-call: asterix:scan-collection, Args:[%0->$$26] -- |UNPARTITIONED|
        group by ([$$43 := %0->$$35]) decor ([%0->$$5; %0->$$37; %0->$$7]) {
                  aggregate [$$26] <- [function-call: asterix:listify, Args:[%0->$$22]] -- |UNPARTITIONED|
                    select (function-call: algebricks:not-null, Args:[%0->$$42]) -- |UNPARTITIONED|
                      nested tuple source -- |UNPARTITIONED|
               } -- |UNPARTITIONED|
          left outer join (function-call: algebricks:eq, Args:[%0->$$35, %0->$$30]) -- |UNPARTITIONED|
            assign [$$7] <- [function-call: asterix:field-access-by-index, Args:[%0->$$5, AInt32: {1}]] -- |UNPARTITIONED|
              assign [$$37] <- [function-call: asterix:field-access-by-name, Args:[%0->$$5, AString: {subscription-id}]] -- |UNPARTITIONED|
                data-scan []<-[$$35, $$5] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                  empty-tuple-source -- |UNPARTITIONED|
            assign [$$42] <- [TRUE] -- |UNPARTITIONED|
              join (TRUE) -- |UNPARTITIONED|
                select (%0->$$21) -- |UNPARTITIONED|
                  group by ([$$30 := %0->$$41]) decor ([%0->$$39; %0->$$38; %0->$$8; %0->$$31]) {
                            aggregate [$$21] <- [function-call: asterix:non-empty-stream, Args:[]] -- |UNPARTITIONED|
                              select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$34]]) -- |UNPARTITIONED|
                                nested tuple source -- |UNPARTITIONED|
                         } -- |UNPARTITIONED|
                    left outer join (function-call: algebricks:eq, Args:[%0->$$36, %0->$$38]) -- |UNPARTITIONED|
                      data-scan []<-[$$31, $$8] <- emergencyTest:CHPReports -- |UNPARTITIONED|
                        assign [$$38] <- [function-call: asterix:field-access-by-index, Args:[%0->$$39, AInt32: {1}]] -- |UNPARTITIONED|
                          assign [$$40] <- [function-call: asterix:field-access-by-name, Args:[%0->$$39, AString: {subscription-id}]] -- |UNPARTITIONED|
                            data-scan []<-[$$41, $$39] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                              empty-tuple-source -- |UNPARTITIONED|
                      assign [$$34] <- [TRUE] -- |UNPARTITIONED|
                        assign [$$36] <- [function-call: asterix:field-access-by-index, Args:[%0->$$10, AInt32: {1}]] -- |UNPARTITIONED|
                          data-scan []<-[$$32, $$10] <- emergencyTest:userLocations -- |UNPARTITIONED|
                            empty-tuple-source -- |UNPARTITIONED|
                assign [$$22] <- [function-call: asterix:open-record-constructor, Args:[AString: {shelter locations}, %0->$$25]] -- |UNPARTITIONED|
                  aggregate [$$25] <- [function-call: asterix:listify, Args:[%0->$$24]] -- |UNPARTITIONED|
                    assign [$$24] <- [function-call: asterix:field-access-by-index, Args:[%0->$$11, AInt32: {1}]] -- |UNPARTITIONED|
                      data-scan []<-[$$33, $$11] <- emergencyTest:tornadoShelters -- |UNPARTITIONED|
                        empty-tuple-source -- |UNPARTITIONED|
--------------------------------

---------- Example 2 -----------
FINE: >>>> Before plan
distribute result [%0->$$8] -- |UNPARTITIONED|
  project ([$$8]) -- |UNPARTITIONED|
    unnest $$8 <- function-call: asterix:scan-collection, Args:[%0->$$41] -- |UNPARTITIONED|
      subplan {
                aggregate [$$41] <- [function-call: asterix:listify, Args:[%0->$$38]] -- |UNPARTITIONED|
                  assign [$$38] <- [function-call: asterix:open-record-constructor, Args:[AString: {subscription-id}, %0->$$54, AString: {execution-time}, function-call: asterix:current-datetime, Args:[], AString: {result}, %0->$$19]] -- |UNPARTITIONED|
                    unnest $$19 <- function-call: asterix:scan-collection, Args:[%0->$$37] -- |UNPARTITIONED|
                      subplan {
                                aggregate [$$37] <- [function-call: asterix:listify, Args:[%0->$$33]] -- |UNPARTITIONED|
                                  join (TRUE) -- |UNPARTITIONED|
                                    select (%0->$$32) -- |UNPARTITIONED|
                                      group by ([$$16 := %0->$$47; $$43 := %0->$$48; $$15 := %0->$$49]) decor ([%0->$$7; %0->$$42; %0->$$14]) {
                                                aggregate [$$32] <- [function-call: asterix:non-empty-stream, Args:[]] -- |UNPARTITIONED|
                                                  select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$46]]) -- |UNPARTITIONED|
                                                    nested tuple source -- |UNPARTITIONED|
                                             } -- |UNPARTITIONED|
                                        left outer join (function-call: algebricks:and, Args:[function-call: algebricks:eq, Args:[%0->$$50, %0->$$14], function-call: asterix:spatial-intersect, Args:[%0->$$47, %0->$$53]]) -- |UNPARTITIONED|
                                          assign [$$47] <- [function-call: asterix:create-circle, Args:[%0->$$51, %0->$$52]] -- |UNPARTITIONED|
                                            assign [$$51] <- [function-call: asterix:field-access-by-index, Args:[%0->$$49, AInt32: {1}]] -- |UNPARTITIONED|
                                              assign [$$52] <- [function-call: asterix:field-access-by-index, Args:[%0->$$49, AInt32: {2}]] -- |UNPARTITIONED|
                                                data-scan []<-[$$48, $$49] <- emergencyTest:CHPReports -- |UNPARTITIONED|
                                                  nested tuple source -- |UNPARTITIONED|
                                          assign [$$46] <- [TRUE] -- |UNPARTITIONED|
                                            assign [$$50] <- [function-call: asterix:field-access-by-index, Args:[%0->$$17, AInt32: {1}]] -- |UNPARTITIONED|
                                              assign [$$53] <- [function-call: asterix:field-access-by-index, Args:[%0->$$17, AInt32: {2}]] -- |UNPARTITIONED|
                                                data-scan []<-[$$44, $$17] <- emergencyTest:userLocations -- |UNPARTITIONED|
                                                  empty-tuple-source -- |UNPARTITIONED|
                                    assign [$$33] <- [function-call: asterix:open-record-constructor, Args:[AString: {shelter locations}, %0->$$36]] -- |UNPARTITIONED|
                                      aggregate [$$36] <- [function-call: asterix:listify, Args:[%0->$$35]] -- |UNPARTITIONED|
                                        assign [$$35] <- [function-call: asterix:field-access-by-index, Args:[%0->$$18, AInt32: {1}]] -- |UNPARTITIONED|
                                          data-scan []<-[$$45, $$18] <- emergencyTest:tornadoShelters -- |UNPARTITIONED|
                                            empty-tuple-source -- |UNPARTITIONED|
                             } -- |UNPARTITIONED|
                        nested tuple source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        assign [$$14] <- [function-call: asterix:field-access-by-index, Args:[%0->$$7, AInt32: {1}]] -- |UNPARTITIONED|
          assign [$$54] <- [function-call: asterix:field-access-by-name, Args:[%0->$$7, AString: {subscription-id}]] -- |UNPARTITIONED|
            data-scan []<-[$$42, $$7] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
              empty-tuple-source -- |UNPARTITIONED|


Dec 28, 2015 12:48:30 PM org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController printRuleApplication
FINE: >>>> After plan
distribute result [%0->$$8] -- |UNPARTITIONED|
  project ([$$8]) -- |UNPARTITIONED|
    unnest $$8 <- function-call: asterix:scan-collection, Args:[%0->$$41] -- |UNPARTITIONED|
      group by ([$$60 := %0->$$42]) decor ([%0->$$54; %0->$$7; %0->$$14]) {
                aggregate [$$41] <- [function-call: asterix:listify, Args:[%0->$$38]] -- |UNPARTITIONED|
                  select (function-call: algebricks:not-null, Args:[%0->$$59]) -- |UNPARTITIONED|
                    nested tuple source -- |UNPARTITIONED|
             } -- |UNPARTITIONED|
        left outer join (function-call: algebricks:eq, Args:[%0->$$42, %0->$$58]) -- |UNPARTITIONED|
          assign [$$14] <- [function-call: asterix:field-access-by-index, Args:[%0->$$7, AInt32: {1}]] -- |UNPARTITIONED|
            assign [$$54] <- [function-call: asterix:field-access-by-name, Args:[%0->$$7, AString: {subscription-id}]] -- |UNPARTITIONED|
              data-scan []<-[$$42, $$7] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                empty-tuple-source -- |UNPARTITIONED|
          assign [$$59] <- [TRUE] -- |UNPARTITIONED|
            assign [$$38] <- [function-call: asterix:open-record-constructor, Args:[AString: {subscription-id}, %0->$$57, AString: {execution-time}, function-call: asterix:current-datetime, Args:[], AString: {result}, %0->$$19]] -- |UNPARTITIONED|
              unnest $$19 <- function-call: asterix:scan-collection, Args:[%0->$$37] -- |UNPARTITIONED|
                group by ([$$66 := %0->$$58]) decor ([%0->$$55; %0->$$56; %0->$$57]) {
                          aggregate [$$37] <- [function-call: asterix:listify, Args:[%0->$$33]] -- |UNPARTITIONED|
                            select (function-call: algebricks:not-null, Args:[%0->$$65]) -- |UNPARTITIONED|
                              nested tuple source -- |UNPARTITIONED|
                       } -- |UNPARTITIONED|
                  left outer join (function-call: algebricks:eq, Args:[%0->$$58, %0->$$64]) -- |UNPARTITIONED|
                    assign [$$55] <- [function-call: asterix:field-access-by-index, Args:[%0->$$56, AInt32: {1}]] -- |UNPARTITIONED|
                      assign [$$57] <- [function-call: asterix:field-access-by-name, Args:[%0->$$56, AString: {subscription-id}]] -- |UNPARTITIONED|
                        data-scan []<-[$$58, $$56] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                          empty-tuple-source -- |UNPARTITIONED|
                    assign [$$65] <- [TRUE] -- |UNPARTITIONED|
                      join (TRUE) -- |UNPARTITIONED|
                        select (%0->$$32) -- |UNPARTITIONED|
                          group by ([$$16 := %0->$$47; $$43 := %0->$$48; $$15 := %0->$$49]) decor ([%0->$$62; %0->$$64; %0->$$61]) {
                                    aggregate [$$32] <- [function-call: asterix:non-empty-stream, Args:[]] -- |UNPARTITIONED|
                                      select (function-call: algebricks:not, Args:[function-call: algebricks:is-null, Args:[%0->$$46]]) -- |UNPARTITIONED|
                                        nested tuple source -- |UNPARTITIONED|
                                 } -- |UNPARTITIONED|
                            left outer join (function-call: algebricks:and, Args:[function-call: algebricks:eq, Args:[%0->$$50, %0->$$61], function-call: asterix:spatial-intersect, Args:[%0->$$47, %0->$$53]]) -- |UNPARTITIONED|
                              assign [$$47] <- [function-call: asterix:create-circle, Args:[%0->$$51, %0->$$52]] -- |UNPARTITIONED|
                                assign [$$51] <- [function-call: asterix:field-access-by-index, Args:[%0->$$49, AInt32: {1}]] -- |UNPARTITIONED|
                                  assign [$$52] <- [function-call: asterix:field-access-by-index, Args:[%0->$$49, AInt32: {2}]] -- |UNPARTITIONED|
                                    data-scan []<-[$$48, $$49] <- emergencyTest:CHPReports -- |UNPARTITIONED|
                                      assign [$$61] <- [function-call: asterix:field-access-by-index, Args:[%0->$$62, AInt32: {1}]] -- |UNPARTITIONED|
                                        assign [$$63] <- [function-call: asterix:field-access-by-name, Args:[%0->$$62, AString: {subscription-id}]] -- |UNPARTITIONED|
                                          data-scan []<-[$$64, $$62] <- emergencyTest:NearbySheltersDuringTornadoDangerChannelSubscriptions -- |UNPARTITIONED|
                                            empty-tuple-source -- |UNPARTITIONED|
                              assign [$$46] <- [TRUE] -- |UNPARTITIONED|
                                assign [$$50] <- [function-call: asterix:field-access-by-index, Args:[%0->$$17, AInt32: {1}]] -- |UNPARTITIONED|
                                  assign [$$53] <- [function-call: asterix:field-access-by-index, Args:[%0->$$17, AInt32: {2}]] -- |UNPARTITIONED|
                                    data-scan []<-[$$44, $$17] <- emergencyTest:userLocations -- |UNPARTITIONED|
                                      empty-tuple-source -- |UNPARTITIONED|
                        assign [$$33] <- [function-call: asterix:open-record-constructor, Args:[AString: {shelter locations}, %0->$$36]] -- |UNPARTITIONED|
                          aggregate [$$36] <- [function-call: asterix:listify, Args:[%0->$$35]] -- |UNPARTITIONED|
                            assign [$$35] <- [function-call: asterix:field-access-by-index, Args:[%0->$$18, AInt32: {1}]] -- |UNPARTITIONED|
                              data-scan []<-[$$45, $$18] <- emergencyTest:tornadoShelters -- |UNPARTITIONED|
                                empty-tuple-source -- |UNPARTITIONED|
---------------------------------
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

        SubplanOperator subplanOp = (SubplanOperator) op;
        if (!containsDataSourceScan(subplanOp)) {
            // Traverses the operator as if it is not a subplan.
            return traverseNonSubplanOperator(op, context);
        }

        Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(0);
        ILogicalOperator inputOp = inputOpRef.getValue();
        Map<LogicalVariable, LogicalVariable> varMap = inlineNestedTupleSource(subplanOp, inputOp, context);

        // Creates parameters for the left outer join operator.
        Set<LogicalVariable> inputLiveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(inputOp, inputLiveVars);
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses((AbstractLogicalOperator) inputOp, context);
        List<FunctionalDependency> fds = context.getFDList(inputOp);
        Set<LogicalVariable> fdCoveringVars = findFDHeaderVariables(fds, inputLiveVars);

        Mutable<ILogicalOperator> rightInputOpRef = subplanOp.getNestedPlans().get(0).getRoots().get(0).getValue()
                .getInputs().get(0);
        ILogicalOperator rightInputOp = rightInputOpRef.getValue();

        Set<LogicalVariable> rightInputLiveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(rightInputOp, rightInputLiveVars);
        Set<LogicalVariable> rightMissingCoveringVars = new HashSet<>();
        Set<LogicalVariable> varsToEnforce = new HashSet<>();
        for (LogicalVariable liveVar : fdCoveringVars) {
            LogicalVariable rightVar = varMap.get(liveVar);
            if (!rightInputLiveVars.contains(rightVar)) {
                // Some correlated variables killed in the subplan, therefore needs to be preserved in the subplan.
                varsToEnforce.add(rightVar);
                rightMissingCoveringVars.add(liveVar);
            }
        }
        // Recovers killed-variables in leftVars in the query plan rooted at rightInputOp.
        if (!varsToEnforce.isEmpty()) {
            Map<LogicalVariable, LogicalVariable> map = VariableUtilities
                    .enforceVariablesInDescendantsAndSelf(rightInputOpRef, varsToEnforce, context);
            // Re-maps variables in the left input branch to the variables in the right input branch
            for (LogicalVariable var : rightMissingCoveringVars) {
                LogicalVariable rightVar = varMap.get(var);
                LogicalVariable newVar = map.get(rightVar);
                if (newVar != null) {
                    varMap.put(var, newVar);
                }
            }
        }

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
        Map<LogicalVariable, LogicalVariable> gbyVarMap = new HashMap<LogicalVariable, LogicalVariable>();
        for (LogicalVariable liveVar : fdCoveringVars) {
            LogicalVariable newVar = context.newVar();
            gbyVarMap.put(liveVar, newVar);
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
        aggOpRef.getValue().getInputs().add(new MutableObject<ILogicalOperator>(selectOp));

        selectOp.getInputs().add(new MutableObject<ILogicalOperator>(
                new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(groupbyOp))));
        List<Mutable<ILogicalOperator>> nestedRoots = new ArrayList<Mutable<ILogicalOperator>>();
        nestedRoots.add(aggOpRef);
        nestedPlans.add(new ALogicalPlanImpl(nestedRoots));
        groupbyOp.getInputs().add(new MutableObject<ILogicalOperator>(leftOuterJoinOp));
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(aggOpRef.getValue(), context);

        // Replaces subplan with the group-by operator.
        opRef.setValue(groupbyOp);
        context.computeAndSetTypeEnvironmentForOperator(groupbyOp);

        // Recursively applys this rule to the nested plan of the subplan operator,
        // for the case where there are nested subplan operators within {@code subplanOp}.
        // Note that we do not need to use the resulting variable map to further replace variables,
        // because rightInputOp must be an aggregate operator which kills all incoming variables.
        traverseNonSubplanOperator(rightInputOp, context);
        return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(true, replacedVarMap);
    }

    /**
     * @param subplanOp
     *            a SubplanOperator
     * @return whether there is a data source scan in the nested logical plans of {@code subplanOp}.
     */
    private boolean containsDataSourceScan(SubplanOperator subplanOp) {
        List<ILogicalPlan> nestedPlans = subplanOp.getNestedPlans();
        for (ILogicalPlan nestedPlan : nestedPlans) {
            for (Mutable<ILogicalOperator> opRef : nestedPlan.getRoots()) {
                if (containsDataScanInDescendantsAndSelf(opRef.getValue())) {
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
    private boolean containsDataScanInDescendantsAndSelf(ILogicalOperator currentOp) {
        if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            return true;
        }
        if (currentOp.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            if (containsDataSourceScan((SubplanOperator) currentOp)) {
                return true;
            }
        }
        for (Mutable<ILogicalOperator> childRef : currentOp.getInputs()) {
            if (containsDataScanInDescendantsAndSelf(childRef.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find the header variables that can imply all the variables in {@code liveVars}
     *
     * @param fds,
     *            a list of functional dependencies
     * @param liveVars,
     *            a set of live variables
     * @return a set of covering variables that can imply all live variables.
     */
    private Set<LogicalVariable> findFDHeaderVariables(List<FunctionalDependency> fds, Set<LogicalVariable> liveVars) {
        Set<LogicalVariable> key = new HashSet<>();
        Set<LogicalVariable> cover = new HashSet<>();
        for (FunctionalDependency fd : fds) {
            key.addAll(fd.getHead());
            cover.addAll(fd.getTail());
        }
        if (cover.equals(liveVars)) {
            return key;
        } else {
            return liveVars;
        }
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
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : replacedVarMap.entrySet()) {
            VariableUtilities.substituteVariables(op, entry.getKey(), entry.getValue(), context);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        return new Pair<Boolean, Map<LogicalVariable, LogicalVariable>>(changed, replacedVarMapForAncestor);
    }

    /**
     * Inline a subplan's input to replace the NTSs.
     *
     * @param subplanOp
     *            the subplan operator
     * @param inputOperator
     *            the input operator to the subplan
     * @param context
     *            the optimization context
     * @return a map that maps from the variables propagated in {@code inputOperator} to the variables
     *         defined in the deeply copied query plan.
     * @throws AlgebricksException
     */
    private Map<LogicalVariable, LogicalVariable> inlineNestedTupleSource(SubplanOperator subplanOp,
            ILogicalOperator inputOperator, IOptimizationContext context) throws AlgebricksException {
        List<ILogicalPlan> nestedPlans = subplanOp.getNestedPlans();
        Map<LogicalVariable, LogicalVariable> varMap = new HashMap<LogicalVariable, LogicalVariable>();
        for (ILogicalPlan plan : nestedPlans) {
            List<Mutable<ILogicalOperator>> roots = plan.getRoots();
            for (Mutable<ILogicalOperator> root : roots) {
                varMap.putAll(replaceNestedTupleSource(root, inputOperator, context));
            }
        }
        return varMap;
    }

    /**
     * Deep copy the query plan rooted at {@code inputOperator} and replace NTS with the copied plan.
     *
     * @param currentInputOpRef,
     *            the current operator within a subplan
     * @param inputOperator,
     *            the input operator to the subplan
     * @param context
     *            the optimization context
     * @return a map that maps from the variables propagated in {@code inputOperator} to the variables
     *         defined in the deeply copied query plan.
     * @throws AlgebricksException
     */
    private Map<LogicalVariable, LogicalVariable> replaceNestedTupleSource(Mutable<ILogicalOperator> currentInputOpRef,
            ILogicalOperator inputOperator, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) currentInputOpRef.getValue();
        if (currentOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            LogicalOperatorDeepCopyWithNewVariablesVisitor deepCopyVisitor = new LogicalOperatorDeepCopyWithNewVariablesVisitor(
                    context);
            ILogicalOperator copiedInputOperator = deepCopyVisitor.deepCopy(inputOperator, inputOperator);
            // Updates the primary key info in the copied plan segment.
            context.updatePrimaryKeys(deepCopyVisitor.getInputToOutputVariableMapping());
            currentInputOpRef.setValue(copiedInputOperator);
            return deepCopyVisitor.getInputToOutputVariableMapping();
        }
        // Obtains the variable mapping from child.
        Map<LogicalVariable, LogicalVariable> varMap = new HashMap<LogicalVariable, LogicalVariable>();
        for (Mutable<ILogicalOperator> child : currentOp.getInputs()) {
            varMap.putAll(replaceNestedTupleSource(child, inputOperator, context));
        }
        // Substitutes variables in the query plan rooted at currentOp.
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : varMap.entrySet()) {
            VariableUtilities.substituteVariables(currentOp, entry.getKey(), entry.getValue(), context);
        }
        return varMap;
    }

}
