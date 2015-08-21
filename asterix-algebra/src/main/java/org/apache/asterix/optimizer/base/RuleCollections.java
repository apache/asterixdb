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

package edu.uci.ics.asterix.optimizer.base;

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.asterix.optimizer.rules.AddEquivalenceClassForRecordConstructorRule;
import edu.uci.ics.asterix.optimizer.rules.AsterixExtractFunctionsFromJoinConditionRule;
import edu.uci.ics.asterix.optimizer.rules.AsterixInlineVariablesRule;
import edu.uci.ics.asterix.optimizer.rules.AsterixIntroduceGroupByCombinerRule;
import edu.uci.ics.asterix.optimizer.rules.AsterixMoveFreeVariableOperatorOutOfSubplanRule;
import edu.uci.ics.asterix.optimizer.rules.ByNameToByIndexFieldAccessRule;
import edu.uci.ics.asterix.optimizer.rules.CancelUnnestWithNestedListifyRule;
import edu.uci.ics.asterix.optimizer.rules.CheckFilterExpressionTypeRule;
import edu.uci.ics.asterix.optimizer.rules.ConstantFoldingRule;
import edu.uci.ics.asterix.optimizer.rules.CountVarToCountOneRule;
import edu.uci.ics.asterix.optimizer.rules.DisjunctivePredicateToJoinRule;
import edu.uci.ics.asterix.optimizer.rules.ExtractDistinctByExpressionsRule;
import edu.uci.ics.asterix.optimizer.rules.ExtractOrderExpressionsRule;
import edu.uci.ics.asterix.optimizer.rules.FeedScanCollectionToUnnest;
import edu.uci.ics.asterix.optimizer.rules.FuzzyEqRule;
import edu.uci.ics.asterix.optimizer.rules.IfElseToSwitchCaseFunctionRule;
import edu.uci.ics.asterix.optimizer.rules.InlineUnnestFunctionRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceAutogenerateIDRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceDynamicTypeCastForExternalFunctionRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceDynamicTypeCastRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceEnforcedListTypeRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceInstantLockSearchCallbackRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceMaterializationForInsertWithSelfScanRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceRandomPartitioningFeedComputationRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceRapidFrameFlushProjectAssignRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceSecondaryIndexInsertDeleteRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceStaticTypeCastForInsertRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceUnionRule;
import edu.uci.ics.asterix.optimizer.rules.IntroduceUnnestForCollectionToSequenceRule;
import edu.uci.ics.asterix.optimizer.rules.LoadRecordFieldsRule;
import edu.uci.ics.asterix.optimizer.rules.NestGroupByRule;
import edu.uci.ics.asterix.optimizer.rules.PushAggFuncIntoStandaloneAggregateRule;
import edu.uci.ics.asterix.optimizer.rules.PushAggregateIntoGroupbyRule;
import edu.uci.ics.asterix.optimizer.rules.PushFieldAccessRule;
import edu.uci.ics.asterix.optimizer.rules.PushGroupByThroughProduct;
import edu.uci.ics.asterix.optimizer.rules.PushProperJoinThroughProduct;
import edu.uci.ics.asterix.optimizer.rules.PushSimilarityFunctionsBelowJoin;
import edu.uci.ics.asterix.optimizer.rules.RemoveRedundantListifyRule;
import edu.uci.ics.asterix.optimizer.rules.RemoveRedundantSelectRule;
import edu.uci.ics.asterix.optimizer.rules.RemoveSortInFeedIngestionRule;
import edu.uci.ics.asterix.optimizer.rules.RemoveUnusedOneToOneEquiJoinRule;
import edu.uci.ics.asterix.optimizer.rules.ReplaceSinkOpWithCommitOpRule;
import edu.uci.ics.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import edu.uci.ics.asterix.optimizer.rules.SetClosedRecordConstructorsRule;
import edu.uci.ics.asterix.optimizer.rules.SimilarityCheckRule;
import edu.uci.ics.asterix.optimizer.rules.SweepIllegalNonfunctionalFunctions;
import edu.uci.ics.asterix.optimizer.rules.UnnestToDataScanRule;
import edu.uci.ics.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import edu.uci.ics.asterix.optimizer.rules.am.IntroduceLSMComponentFilterRule;
import edu.uci.ics.asterix.optimizer.rules.am.IntroduceSelectAccessMethodRule;
import edu.uci.ics.asterix.optimizer.rules.temporal.TranslateIntervalExpressionRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ComplexJoinInferenceRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ComplexUnnestToProductRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.CopyLimitDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EliminateGroupByEmptyKeyRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EliminateSubplanRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EliminateSubplanWithInputCardinalityOneRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EnforceOrderByAfterSubplan;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ExtractCommonExpressionsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InferTypesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineAssignIntoAggregateRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineSingleReferenceVariablesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InsertOuterJoinRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InsertProjectBeforeUnionRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroHashPartitionMergeExchange;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroJoinInsideSubplanRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroduceGroupByForSubplanRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.NestedSubplanToJoinRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushAssignBelowUnionAllRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushGroupByIntoSortRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushMapOperatorDownThroughProductRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushNestedOrderByUnderPreSortedGroupByRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSubplanIntoGroupByRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushSubplanWithAggregateDownThroughProductRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushUnnestDownThroughUnionRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.RemoveRedundantGroupByDecorVars;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SimpleUnnestToProductRule;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.SubplanOutOfGroupRule;

public final class RuleCollections {

    public final static List<IAlgebraicRewriteRule> buildInitialTranslationRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<IAlgebraicRewriteRule>();
        typeInfer.add(new TranslateIntervalExpressionRule());
        return typeInfer;
    }

    public final static List<IAlgebraicRewriteRule> buildTypeInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<IAlgebraicRewriteRule>();
        typeInfer.add(new InlineUnnestFunctionRule());
        typeInfer.add(new InferTypesRule());
        typeInfer.add(new CheckFilterExpressionTypeRule());
        return typeInfer;
    }

    public final static List<IAlgebraicRewriteRule> buildAutogenerateIDRuleCollection() {
        List<IAlgebraicRewriteRule> autogen = new LinkedList<>();
        autogen.add(new IntroduceAutogenerateIDRule());
        return autogen;
    }

    public final static List<IAlgebraicRewriteRule> buildNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();
        normalization.add(new IntroduceUnnestForCollectionToSequenceRule());
        normalization.add(new EliminateSubplanRule());
        normalization.add(new EnforceOrderByAfterSubplan());
        normalization.add(new PushAggFuncIntoStandaloneAggregateRule());
        normalization.add(new BreakSelectIntoConjunctsRule());
        normalization.add(new ExtractGbyExpressionsRule());
        normalization.add(new ExtractDistinctByExpressionsRule());
        normalization.add(new ExtractOrderExpressionsRule());
        normalization.add(new AsterixMoveFreeVariableOperatorOutOfSubplanRule());

        // IntroduceStaticTypeCastRule should go before
        // IntroduceDynamicTypeCastRule to
        // avoid unnecessary dynamic casting
        normalization.add(new IntroduceStaticTypeCastForInsertRule());
        normalization.add(new IntroduceDynamicTypeCastRule());
        normalization.add(new IntroduceDynamicTypeCastForExternalFunctionRule());
        normalization.add(new IntroduceEnforcedListTypeRule());
        normalization.add(new ExtractCommonExpressionsRule());
        normalization.add(new ConstantFoldingRule());
        normalization.add(new RemoveRedundantSelectRule());
        normalization.add(new UnnestToDataScanRule());
        normalization.add(new IfElseToSwitchCaseFunctionRule());
        normalization.add(new FuzzyEqRule());
        normalization.add(new SimilarityCheckRule());
        return normalization;
    }

    public final static List<IAlgebraicRewriteRule> buildCondPushDownAndJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> condPushDownAndJoinInference = new LinkedList<IAlgebraicRewriteRule>();

        condPushDownAndJoinInference.add(new PushSelectDownRule());
        condPushDownAndJoinInference.add(new RemoveRedundantListifyRule());
        condPushDownAndJoinInference.add(new CancelUnnestWithNestedListifyRule());
        condPushDownAndJoinInference.add(new SimpleUnnestToProductRule());
        condPushDownAndJoinInference.add(new ComplexUnnestToProductRule());
        condPushDownAndJoinInference.add(new ComplexJoinInferenceRule());
        condPushDownAndJoinInference.add(new DisjunctivePredicateToJoinRule());
        condPushDownAndJoinInference.add(new PushSelectIntoJoinRule());
        condPushDownAndJoinInference.add(new IntroJoinInsideSubplanRule());
        condPushDownAndJoinInference.add(new PushMapOperatorDownThroughProductRule());
        condPushDownAndJoinInference.add(new PushSubplanWithAggregateDownThroughProductRule());
        condPushDownAndJoinInference.add(new IntroduceGroupByForSubplanRule());
        condPushDownAndJoinInference.add(new SubplanOutOfGroupRule());
        condPushDownAndJoinInference.add(new InsertOuterJoinRule());
        condPushDownAndJoinInference.add(new AsterixExtractFunctionsFromJoinConditionRule());

        condPushDownAndJoinInference.add(new RemoveRedundantVariablesRule());
        condPushDownAndJoinInference.add(new AsterixInlineVariablesRule());
        condPushDownAndJoinInference.add(new RemoveUnusedAssignAndAggregateRule());

        condPushDownAndJoinInference.add(new FactorRedundantGroupAndDecorVarsRule());
        condPushDownAndJoinInference.add(new PushAggregateIntoGroupbyRule());
        condPushDownAndJoinInference.add(new EliminateSubplanRule());
        condPushDownAndJoinInference.add(new PushProperJoinThroughProduct());
        condPushDownAndJoinInference.add(new PushGroupByThroughProduct());
        condPushDownAndJoinInference.add(new NestGroupByRule());
        condPushDownAndJoinInference.add(new EliminateGroupByEmptyKeyRule());
        condPushDownAndJoinInference.add(new PushSubplanIntoGroupByRule());
        condPushDownAndJoinInference.add(new NestedSubplanToJoinRule());
        condPushDownAndJoinInference.add(new EliminateSubplanWithInputCardinalityOneRule());

        return condPushDownAndJoinInference;
    }

    public final static List<IAlgebraicRewriteRule> buildLoadFieldsRuleCollection() {
        List<IAlgebraicRewriteRule> fieldLoads = new LinkedList<IAlgebraicRewriteRule>();
        fieldLoads.add(new LoadRecordFieldsRule());
        fieldLoads.add(new PushFieldAccessRule());
        // fieldLoads.add(new ByNameToByHandleFieldAccessRule()); -- disabled
        fieldLoads.add(new ByNameToByIndexFieldAccessRule());
        fieldLoads.add(new RemoveRedundantVariablesRule());
        fieldLoads.add(new AsterixInlineVariablesRule());
        fieldLoads.add(new RemoveUnusedAssignAndAggregateRule());
        fieldLoads.add(new ConstantFoldingRule());
        fieldLoads.add(new FeedScanCollectionToUnnest());
        fieldLoads.add(new ComplexJoinInferenceRule());
        return fieldLoads;
    }

    public final static List<IAlgebraicRewriteRule> buildFuzzyJoinRuleCollection() {
        List<IAlgebraicRewriteRule> fuzzy = new LinkedList<IAlgebraicRewriteRule>();
        // fuzzy.add(new FuzzyJoinRule()); -- The non-indexed fuzzy join will be temporarily disabled. It should be enabled some time in the near future.
        fuzzy.add(new InferTypesRule());
        return fuzzy;
    }

    public final static List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> consolidation = new LinkedList<IAlgebraicRewriteRule>();
        consolidation.add(new ConsolidateSelectsRule());
        consolidation.add(new ConsolidateAssignsRule());
        consolidation.add(new InlineAssignIntoAggregateRule());
        consolidation.add(new AsterixIntroduceGroupByCombinerRule());
        consolidation.add(new IntroduceAggregateCombinerRule());
        consolidation.add(new CountVarToCountOneRule());
        consolidation.add(new RemoveUnusedAssignAndAggregateRule());
        consolidation.add(new RemoveRedundantGroupByDecorVars());
        consolidation.add(new NestedSubplanToJoinRule());
        //unionRule => PushUnnestDownUnion => RemoveRedundantListifyRule cause these rules are correlated
        consolidation.add(new IntroduceUnionRule());
        consolidation.add(new PushUnnestDownThroughUnionRule());
        consolidation.add(new RemoveRedundantListifyRule());

        return consolidation;
    }

    public final static List<IAlgebraicRewriteRule> buildAccessMethodRuleCollection() {
        List<IAlgebraicRewriteRule> accessMethod = new LinkedList<IAlgebraicRewriteRule>();
        accessMethod.add(new IntroduceSelectAccessMethodRule());
        accessMethod.add(new IntroduceJoinAccessMethodRule());
        accessMethod.add(new IntroduceLSMComponentFilterRule());
        accessMethod.add(new IntroduceSecondaryIndexInsertDeleteRule());
        accessMethod.add(new RemoveUnusedOneToOneEquiJoinRule());
        accessMethod.add(new PushSimilarityFunctionsBelowJoin());
        accessMethod.add(new RemoveUnusedAssignAndAggregateRule());
        return accessMethod;
    }

    public final static List<IAlgebraicRewriteRule> buildPlanCleanupRuleCollection() {
        List<IAlgebraicRewriteRule> planCleanupRules = new LinkedList<IAlgebraicRewriteRule>();
        planCleanupRules.add(new PushAssignBelowUnionAllRule());
        planCleanupRules.add(new ExtractCommonExpressionsRule());
        planCleanupRules.add(new RemoveRedundantVariablesRule());
        planCleanupRules.add(new PushProjectDownRule());
        planCleanupRules.add(new PushSelectDownRule());
        planCleanupRules.add(new SetClosedRecordConstructorsRule());
        planCleanupRules.add(new IntroduceDynamicTypeCastRule());
        planCleanupRules.add(new IntroduceDynamicTypeCastForExternalFunctionRule());
        planCleanupRules.add(new RemoveUnusedAssignAndAggregateRule());
        return planCleanupRules;
    }

    public final static List<IAlgebraicRewriteRule> buildDataExchangeRuleCollection() {
        List<IAlgebraicRewriteRule> dataExchange = new LinkedList<IAlgebraicRewriteRule>();
        dataExchange.add(new SetExecutionModeRule());
        return dataExchange;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesAllLevelsRuleCollection() {
        List<IAlgebraicRewriteRule> physicalRewritesAllLevels = new LinkedList<IAlgebraicRewriteRule>();
        physicalRewritesAllLevels.add(new PullSelectOutOfEqJoin());
        //Turned off the following rule for now not to change OptimizerTest results.
        //physicalRewritesAllLevels.add(new IntroduceTransactionCommitByAssignOpRule());
        physicalRewritesAllLevels.add(new ReplaceSinkOpWithCommitOpRule());
        physicalRewritesAllLevels.add(new SetAlgebricksPhysicalOperatorsRule());
        physicalRewritesAllLevels.add(new SetAsterixPhysicalOperatorsRule());
        physicalRewritesAllLevels.add(new IntroduceInstantLockSearchCallbackRule());
        physicalRewritesAllLevels.add(new AddEquivalenceClassForRecordConstructorRule());
        physicalRewritesAllLevels.add(new EnforceStructuralPropertiesRule());
        physicalRewritesAllLevels.add(new RemoveSortInFeedIngestionRule());
        physicalRewritesAllLevels.add(new IntroHashPartitionMergeExchange());
        physicalRewritesAllLevels.add(new PushProjectDownRule());
        physicalRewritesAllLevels.add(new InsertProjectBeforeUnionRule());
        physicalRewritesAllLevels.add(new IntroduceMaterializationForInsertWithSelfScanRule());
        physicalRewritesAllLevels.add(new InlineSingleReferenceVariablesRule());
        physicalRewritesAllLevels.add(new RemoveUnusedAssignAndAggregateRule());
        physicalRewritesAllLevels.add(new ConsolidateAssignsRule());
        // After adding projects, we may need need to set physical operators again.
        physicalRewritesAllLevels.add(new SetAlgebricksPhysicalOperatorsRule());
        return physicalRewritesAllLevels;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesTopLevelRuleCollection() {
        List<IAlgebraicRewriteRule> physicalRewritesTopLevel = new LinkedList<IAlgebraicRewriteRule>();
        physicalRewritesTopLevel.add(new PushNestedOrderByUnderPreSortedGroupByRule());
        physicalRewritesTopLevel.add(new CopyLimitDownRule());
        physicalRewritesTopLevel.add(new IntroduceProjectsRule());
        physicalRewritesTopLevel.add(new SetAlgebricksPhysicalOperatorsRule());
        physicalRewritesTopLevel.add(new IntroduceRapidFrameFlushProjectAssignRule());
        physicalRewritesTopLevel.add(new SetExecutionModeRule());
        physicalRewritesTopLevel.add(new IntroduceRandomPartitioningFeedComputationRule());
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
        prepareForJobGenRewrites.add(new PushGroupByIntoSortRule());
        prepareForJobGenRewrites.add(new SetExecutionModeRule());
        prepareForJobGenRewrites.add(new SweepIllegalNonfunctionalFunctions());
        return prepareForJobGenRewrites;
    }
}
