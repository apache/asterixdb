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

package org.apache.asterix.optimizer.base;

import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.optimizer.rules.AddEquivalenceClassForRecordConstructorRule;
import org.apache.asterix.optimizer.rules.AsterixExtractFunctionsFromJoinConditionRule;
import org.apache.asterix.optimizer.rules.AsterixInlineVariablesRule;
import org.apache.asterix.optimizer.rules.AsterixIntroduceGroupByCombinerRule;
import org.apache.asterix.optimizer.rules.AsterixMoveFreeVariableOperatorOutOfSubplanRule;
import org.apache.asterix.optimizer.rules.ByNameToByIndexFieldAccessRule;
import org.apache.asterix.optimizer.rules.CancelUnnestWithNestedListifyRule;
import org.apache.asterix.optimizer.rules.CheckFilterExpressionTypeRule;
import org.apache.asterix.optimizer.rules.ConstantFoldingRule;
import org.apache.asterix.optimizer.rules.CountVarToCountOneRule;
import org.apache.asterix.optimizer.rules.DisjunctivePredicateToJoinRule;
import org.apache.asterix.optimizer.rules.ExtractDistinctByExpressionsRule;
import org.apache.asterix.optimizer.rules.ExtractOrderExpressionsRule;
import org.apache.asterix.optimizer.rules.FeedScanCollectionToUnnest;
import org.apache.asterix.optimizer.rules.FuzzyEqRule;
import org.apache.asterix.optimizer.rules.IfElseToSwitchCaseFunctionRule;
import org.apache.asterix.optimizer.rules.InlineUnnestFunctionRule;
import org.apache.asterix.optimizer.rules.IntroduceAutogenerateIDRule;
import org.apache.asterix.optimizer.rules.IntroduceDynamicTypeCastForExternalFunctionRule;
import org.apache.asterix.optimizer.rules.IntroduceDynamicTypeCastRule;
import org.apache.asterix.optimizer.rules.IntroduceEnforcedListTypeRule;
import org.apache.asterix.optimizer.rules.IntroduceInstantLockSearchCallbackRule;
import org.apache.asterix.optimizer.rules.IntroduceMaterializationForInsertWithSelfScanRule;
import org.apache.asterix.optimizer.rules.IntroduceRandomPartitioningFeedComputationRule;
import org.apache.asterix.optimizer.rules.IntroduceRapidFrameFlushProjectAssignRule;
import org.apache.asterix.optimizer.rules.IntroduceSecondaryIndexInsertDeleteRule;
import org.apache.asterix.optimizer.rules.IntroduceStaticTypeCastForInsertRule;
import org.apache.asterix.optimizer.rules.IntroduceUnionRule;
import org.apache.asterix.optimizer.rules.IntroduceUnnestForCollectionToSequenceRule;
import org.apache.asterix.optimizer.rules.LoadRecordFieldsRule;
import org.apache.asterix.optimizer.rules.NestGroupByRule;
import org.apache.asterix.optimizer.rules.PushAggFuncIntoStandaloneAggregateRule;
import org.apache.asterix.optimizer.rules.PushAggregateIntoGroupbyRule;
import org.apache.asterix.optimizer.rules.PushFieldAccessRule;
import org.apache.asterix.optimizer.rules.PushGroupByThroughProduct;
import org.apache.asterix.optimizer.rules.PushProperJoinThroughProduct;
import org.apache.asterix.optimizer.rules.PushSimilarityFunctionsBelowJoin;
import org.apache.asterix.optimizer.rules.RemoveRedundantListifyRule;
import org.apache.asterix.optimizer.rules.RemoveRedundantSelectRule;
import org.apache.asterix.optimizer.rules.RemoveSortInFeedIngestionRule;
import org.apache.asterix.optimizer.rules.RemoveUnusedOneToOneEquiJoinRule;
import org.apache.asterix.optimizer.rules.ReplaceSinkOpWithCommitOpRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.asterix.optimizer.rules.SetClosedRecordConstructorsRule;
import org.apache.asterix.optimizer.rules.SimilarityCheckRule;
import org.apache.asterix.optimizer.rules.SweepIllegalNonfunctionalFunctions;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.asterix.optimizer.rules.am.IntroduceLSMComponentFilterRule;
import org.apache.asterix.optimizer.rules.am.IntroduceSelectAccessMethodRule;
import org.apache.asterix.optimizer.rules.temporal.TranslateIntervalExpressionRule;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexJoinInferenceRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.CopyLimitDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateGroupByEmptyKeyRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateSubplanWithInputCardinalityOneRule;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceOrderByAfterSubplan;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import org.apache.hyracks.algebricks.rewriter.rules.InferTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineAssignIntoAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineSingleReferenceVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InsertOuterJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.InsertProjectBeforeUnionRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroJoinInsideSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceGroupByForSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.NestedSubplanToJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import org.apache.hyracks.algebricks.rewriter.rules.PushAssignBelowUnionAllRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushGroupByIntoSortRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushMapOperatorDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushNestedOrderByUnderPreSortedGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSortDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanIntoGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanWithAggregateDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushUnnestDownThroughUnionRule;
import org.apache.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantGroupByDecorVars;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnnecessarySortMergeExchange;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;
import org.apache.hyracks.algebricks.rewriter.rules.SimpleUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.SubplanOutOfGroupRule;

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
        condPushDownAndJoinInference.add(new PushSortDownRule());
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
        physicalRewritesAllLevels.add(new RemoveUnnecessarySortMergeExchange());
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
        prepareForJobGenRewrites
                .add(new IsolateHyracksOperatorsRule(HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
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
