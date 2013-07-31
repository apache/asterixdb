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
package edu.uci.ics.hivesterix.optimizer.rulecollections;

import java.util.LinkedList;

import edu.uci.ics.hivesterix.optimizer.rules.InsertProjectBeforeWriteRule;
import edu.uci.ics.hivesterix.optimizer.rules.IntroduceEarlyProjectRule;
import edu.uci.ics.hivesterix.optimizer.rules.LocalGroupByRule;
import edu.uci.ics.hivesterix.optimizer.rules.RemoveRedundantSelectRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ComplexJoinInferenceRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EliminateSubplanRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InferTypesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InsertProjectBeforeUnionRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroduceGroupByCombinerRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushLimitDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushProjectIntoDataSourceScanRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.RemoveRedundantProjectionRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;

public final class HiveRuleCollections {

    public final static LinkedList<IAlgebraicRewriteRule> NORMALIZATION = new LinkedList<IAlgebraicRewriteRule>();
    static {
        NORMALIZATION.add(new EliminateSubplanRule());
        NORMALIZATION.add(new BreakSelectIntoConjunctsRule());
        NORMALIZATION.add(new PushSelectIntoJoinRule());
        NORMALIZATION.add(new ExtractGbyExpressionsRule());
        NORMALIZATION.add(new RemoveRedundantSelectRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> COND_PUSHDOWN_AND_JOIN_INFERENCE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new PushSelectDownRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new InlineVariablesRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new FactorRedundantGroupAndDecorVarsRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new EliminateSubplanRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> LOAD_FIELDS = new LinkedList<IAlgebraicRewriteRule>();
    static {
        // should LoadRecordFieldsRule be applied in only one pass over the
        // plan?
        LOAD_FIELDS.add(new InlineVariablesRule());
        // LOAD_FIELDS.add(new RemoveUnusedAssignAndAggregateRule());
        LOAD_FIELDS.add(new ComplexJoinInferenceRule());
        LOAD_FIELDS.add(new InferTypesRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> OP_PUSHDOWN = new LinkedList<IAlgebraicRewriteRule>();
    static {
        OP_PUSHDOWN.add(new PushProjectDownRule());
        OP_PUSHDOWN.add(new PushSelectDownRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> DATA_EXCHANGE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        DATA_EXCHANGE.add(new SetExecutionModeRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> CONSOLIDATION = new LinkedList<IAlgebraicRewriteRule>();
    static {
        CONSOLIDATION.add(new RemoveRedundantProjectionRule());
        CONSOLIDATION.add(new ConsolidateSelectsRule());
        CONSOLIDATION.add(new IntroduceEarlyProjectRule());
        CONSOLIDATION.add(new ConsolidateAssignsRule());
        CONSOLIDATION.add(new IntroduceGroupByCombinerRule());
        CONSOLIDATION.add(new IntroduceAggregateCombinerRule());
        CONSOLIDATION.add(new RemoveUnusedAssignAndAggregateRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> PHYSICAL_PLAN_REWRITES = new LinkedList<IAlgebraicRewriteRule>();
    static {
        PHYSICAL_PLAN_REWRITES.add(new PullSelectOutOfEqJoin());
        PHYSICAL_PLAN_REWRITES.add(new SetAlgebricksPhysicalOperatorsRule());
        PHYSICAL_PLAN_REWRITES.add(new EnforceStructuralPropertiesRule());
        PHYSICAL_PLAN_REWRITES.add(new PushProjectDownRule());
        PHYSICAL_PLAN_REWRITES.add(new SetAlgebricksPhysicalOperatorsRule());
        PHYSICAL_PLAN_REWRITES.add(new PushLimitDownRule());
        PHYSICAL_PLAN_REWRITES.add(new InsertProjectBeforeWriteRule());
        PHYSICAL_PLAN_REWRITES.add(new InsertProjectBeforeUnionRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> prepareJobGenRules = new LinkedList<IAlgebraicRewriteRule>();
    static {
        prepareJobGenRules.add(new ReinferAllTypesRule());
        prepareJobGenRules.add(new IsolateHyracksOperatorsRule(
                HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
        prepareJobGenRules.add(new ExtractCommonOperatorsRule());
        prepareJobGenRules.add(new LocalGroupByRule());
        prepareJobGenRules.add(new PushProjectIntoDataSourceScanRule());
        prepareJobGenRules.add(new ReinferAllTypesRule());
        prepareJobGenRules.add(new SetExecutionModeRule());
    }

}
