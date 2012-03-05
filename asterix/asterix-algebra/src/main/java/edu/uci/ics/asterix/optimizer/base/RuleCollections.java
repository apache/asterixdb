package edu.uci.ics.asterix.optimizer.base;

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.rewriter.rules.*; 
import edu.uci.ics.asterix.optimizer.rules.AsterixInlineVariablesRule;
import edu.uci.ics.asterix.optimizer.rules.ByNameToByIndexFieldAccessRule;
import edu.uci.ics.asterix.optimizer.rules.ConstantFoldingRule;
import edu.uci.ics.asterix.optimizer.rules.CountVarToCountOneRule;
import edu.uci.ics.asterix.optimizer.rules.ExtractDistinctByExpressionsRule;
import edu.uci.ics.asterix.optimizer.rules.ExtractOrderExpressionsRule;
import edu.uci.ics.asterix.optimizer.rules.FeedScanCollectionToUnnest;
import edu.uci.ics.asterix.optimizer.rules.FuzzyEqRule;
import edu.uci.ics.asterix.optimizer.rules.FuzzyJoinRule;
import edu.uci.ics.asterix.optimizer.rules.IfElseToSwitchCaseFunctionRule;
import edu.uci.ics.asterix.optimizer.rules.InlineAssignIntoAggregateRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceBTreeIndexSearchRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceRTreeIndexSearchRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceSecondaryIndexInsertDeleteRule;
import edu.uci.ics.asterix.optimizer.rules.LoadRecordFieldsRule;
import edu.uci.ics.asterix.optimizer.rules.NestGroupByRule;
import edu.uci.ics.asterix.optimizer.rules.PullPositionalVariableFromUnnestRule;
import edu.uci.ics.asterix.optimizer.rules.PushAggregateIntoGroupbyRule;
import edu.uci.ics.asterix.optimizer.rules.PushFieldAccessRule;
import edu.uci.ics.asterix.optimizer.rules.PushGroupByThroughProduct;
import edu.uci.ics.asterix.optimizer.rules.PushProperJoinThroughProduct;
import edu.uci.ics.asterix.optimizer.rules.RemoveRedundantListifyRule;
import edu.uci.ics.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import edu.uci.ics.asterix.optimizer.rules.SetClosedRecordConstructorsRule;
import edu.uci.ics.asterix.optimizer.rules.UnnestToDataScanRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public final class RuleCollections {

    public final static List<IAlgebraicRewriteRule> buildTypeInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<IAlgebraicRewriteRule>();
        typeInfer.add(new InferTypesRule());
        return typeInfer;
    }

    public final static List<IAlgebraicRewriteRule> buildNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();
        normalization.add(new EliminateSubplanRule());
        normalization.add(new IntroduceGroupByForStandaloneAggregRule());
        normalization.add(new BreakSelectIntoConjunctsRule());
        normalization.add(new ExtractGbyExpressionsRule());
        normalization.add(new ExtractDistinctByExpressionsRule());
        normalization.add(new ExtractOrderExpressionsRule());
        normalization.add(new ConstantFoldingRule());
        normalization.add(new UnnestToDataScanRule());
        normalization.add(new IfElseToSwitchCaseFunctionRule());
        return normalization;
    }

    public final static List<IAlgebraicRewriteRule> buildCondPushDownAndJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> condPushDownAndJoinInference = new LinkedList<IAlgebraicRewriteRule>();

        condPushDownAndJoinInference.add(new PushSelectDownRule());
        condPushDownAndJoinInference.add(new PushDieUpRule());
        condPushDownAndJoinInference.add(new RemoveRedundantListifyRule());
        condPushDownAndJoinInference.add(new SimpleUnnestToProductRule());
        condPushDownAndJoinInference.add(new ComplexJoinInferenceRule());
        condPushDownAndJoinInference.add(new PushSelectIntoJoinRule());
        condPushDownAndJoinInference.add(new IntroJoinInsideSubplanRule());
        condPushDownAndJoinInference.add(new PushAssignDownThroughProductRule());
        condPushDownAndJoinInference.add(new PushSubplanWithAggregateDownThroughProductRule());
        condPushDownAndJoinInference.add(new IntroduceGroupByForSubplanRule());
        condPushDownAndJoinInference.add(new SubplanOutOfGroupRule());
        condPushDownAndJoinInference.add(new InsertOuterJoinRule());
        condPushDownAndJoinInference.add(new AsterixInlineVariablesRule());
        condPushDownAndJoinInference.add(new RemoveUnusedAssignAndAggregateRule());
        condPushDownAndJoinInference.add(new FactorRedundantGroupAndDecorVarsRule());
        condPushDownAndJoinInference.add(new PushAggregateIntoGroupbyRule());
        condPushDownAndJoinInference.add(new EliminateSubplanRule());
        condPushDownAndJoinInference.add(new PushProperJoinThroughProduct());
        condPushDownAndJoinInference.add(new PushGroupByThroughProduct());
        condPushDownAndJoinInference.add(new NestGroupByRule());

        return condPushDownAndJoinInference;
    }

    public final static List<IAlgebraicRewriteRule> buildLoadFieldsRuleCollection() {
        List<IAlgebraicRewriteRule> fieldLoads = new LinkedList<IAlgebraicRewriteRule>();
        fieldLoads.add(new LoadRecordFieldsRule());
        fieldLoads.add(new PushFieldAccessRule());
        // fieldLoads.add(new ByNameToByHandleFieldAccessRule()); -- disabled
        fieldLoads.add(new ByNameToByIndexFieldAccessRule());
        fieldLoads.add(new AsterixInlineVariablesRule());
        // fieldLoads.add(new InlineRecordAccessRule());
        fieldLoads.add(new RemoveUnusedAssignAndAggregateRule());
        fieldLoads.add(new ConstantFoldingRule());
        fieldLoads.add(new FeedScanCollectionToUnnest());
        fieldLoads.add(new ComplexJoinInferenceRule());
        return fieldLoads;
    }

    public final static List<IAlgebraicRewriteRule> buildFuzzyJoinRuleCollection() {
        List<IAlgebraicRewriteRule> fuzzy = new LinkedList<IAlgebraicRewriteRule>();
        fuzzy.add(new FuzzyJoinRule());
        fuzzy.add(new InferTypesRule());
        fuzzy.add(new FuzzyEqRule());
        return fuzzy;
    }

    public final static List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> consolidation = new LinkedList<IAlgebraicRewriteRule>();
        consolidation.add(new ConsolidateSelectsRule());
        consolidation.add(new ConsolidateAssignsRule());
        consolidation.add(new InlineAssignIntoAggregateRule());
        consolidation.add(new IntroduceCombinerRule());
        consolidation.add(new CountVarToCountOneRule());
        consolidation.add(new IntroduceBTreeIndexSearchRule());
        consolidation.add(new IntroduceRTreeIndexSearchRule());
        consolidation.add(new RemoveUnusedAssignAndAggregateRule());
        consolidation.add(new IntroduceSecondaryIndexInsertDeleteRule());
        return consolidation;
    }

    public final static List<IAlgebraicRewriteRule> buildOpPushDownRuleCollection() {
        List<IAlgebraicRewriteRule> opPushDown = new LinkedList<IAlgebraicRewriteRule>();
        opPushDown.add(new PushProjectDownRule());
        opPushDown.add(new PushSelectDownRule());
        return opPushDown;
    }

    public final static List<IAlgebraicRewriteRule> buildDataExchangeRuleCollection() {
        List<IAlgebraicRewriteRule> dataExchange = new LinkedList<IAlgebraicRewriteRule>();
        dataExchange.add(new SetExecutionModeRule());
        return dataExchange;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesAllLevelsRuleCollection() {
        List<IAlgebraicRewriteRule> physicalRewritesAllLevels = new LinkedList<IAlgebraicRewriteRule>();
        physicalRewritesAllLevels.add(new PullSelectOutOfEqJoin());
        physicalRewritesAllLevels.add(new SetAlgebricksPhysicalOperatorsRule());
        physicalRewritesAllLevels.add(new SetAsterixPhysicalOperatorsRule());
        physicalRewritesAllLevels.add(new EnforceStructuralPropertiesRule());
        physicalRewritesAllLevels.add(new IntroHashPartitionMergeExchange());
        physicalRewritesAllLevels.add(new SetClosedRecordConstructorsRule());
        physicalRewritesAllLevels.add(new PullPositionalVariableFromUnnestRule());
        physicalRewritesAllLevels.add(new PushProjectDownRule());
        physicalRewritesAllLevels.add(new InsertProjectBeforeUnionRule());
        return physicalRewritesAllLevels;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesTopLevelRuleCollection() {
        List<IAlgebraicRewriteRule> physicalRewritesTopLevel = new LinkedList<IAlgebraicRewriteRule>();
        physicalRewritesTopLevel.add(new PushNestedOrderByUnderPreSortedGroupByRule());
        physicalRewritesTopLevel.add(new PushLimitDownRule());
        return physicalRewritesTopLevel;
    }

    public final static List<IAlgebraicRewriteRule> prepareForJobGenRuleCollection() {
        List<IAlgebraicRewriteRule> prepareForJobGenRewrites = new LinkedList<IAlgebraicRewriteRule>();
        prepareForJobGenRewrites.add(new IsolateHyracksOperatorsRule(
                HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
        prepareForJobGenRewrites.add(new ExtractCommonOperatorsRule());
        // Re-infer all types, so that, e.g., the effect of not-is-null is
        // propagated.
        prepareForJobGenRewrites.add(new ReinferAllTypesRule());
        return prepareForJobGenRewrites;
    }

}