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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.AddEquivalenceClassForRecordConstructorRule;
import org.apache.asterix.optimizer.rules.AsterixConsolidateWindowOperatorsRule;
import org.apache.asterix.optimizer.rules.AsterixExtractFunctionsFromJoinConditionRule;
import org.apache.asterix.optimizer.rules.AsterixInlineVariablesRule;
import org.apache.asterix.optimizer.rules.AsterixIntroduceGroupByCombinerRule;
import org.apache.asterix.optimizer.rules.AsterixPushAssignBelowUnionAllRule;
import org.apache.asterix.optimizer.rules.ByNameToByIndexFieldAccessRule;
import org.apache.asterix.optimizer.rules.CancelUnnestSingletonListRule;
import org.apache.asterix.optimizer.rules.CancelUnnestWithNestedListifyRule;
import org.apache.asterix.optimizer.rules.CheckFilterExpressionTypeRule;
import org.apache.asterix.optimizer.rules.CheckFullParallelSortRule;
import org.apache.asterix.optimizer.rules.CheckInsertUpsertReturningRule;
import org.apache.asterix.optimizer.rules.ConstantFoldingRule;
import org.apache.asterix.optimizer.rules.CountVarToCountOneRule;
import org.apache.asterix.optimizer.rules.DisjunctivePredicateToJoinRule;
import org.apache.asterix.optimizer.rules.ExtractDistinctByExpressionsRule;
import org.apache.asterix.optimizer.rules.ExtractOrderExpressionsRule;
import org.apache.asterix.optimizer.rules.ExtractWindowExpressionsRule;
import org.apache.asterix.optimizer.rules.FeedScanCollectionToUnnest;
import org.apache.asterix.optimizer.rules.FindDataSourcesRule;
import org.apache.asterix.optimizer.rules.FixReplicateOperatorOutputsRule;
import org.apache.asterix.optimizer.rules.FullTextContainsParameterCheckRule;
import org.apache.asterix.optimizer.rules.FuzzyEqRule;
import org.apache.asterix.optimizer.rules.FuzzyJoinRule;
import org.apache.asterix.optimizer.rules.InjectTypeCastForFunctionArgumentsRule;
import org.apache.asterix.optimizer.rules.InjectTypeCastForUnionRule;
import org.apache.asterix.optimizer.rules.InlineUnnestFunctionRule;
import org.apache.asterix.optimizer.rules.IntroduceAutogenerateIDRule;
import org.apache.asterix.optimizer.rules.IntroduceDynamicTypeCastForExternalFunctionRule;
import org.apache.asterix.optimizer.rules.IntroduceDynamicTypeCastRule;
import org.apache.asterix.optimizer.rules.IntroduceEnforcedListTypeRule;
import org.apache.asterix.optimizer.rules.IntroduceMaterializationForInsertWithSelfScanRule;
import org.apache.asterix.optimizer.rules.IntroduceRandomPartitioningFeedComputationRule;
import org.apache.asterix.optimizer.rules.IntroduceRapidFrameFlushProjectAssignRule;
import org.apache.asterix.optimizer.rules.IntroduceSecondaryIndexInsertDeleteRule;
import org.apache.asterix.optimizer.rules.IntroduceStaticTypeCastForInsertRule;
import org.apache.asterix.optimizer.rules.IntroduceUnnestForCollectionToSequenceRule;
import org.apache.asterix.optimizer.rules.ListifyUnnestingFunctionRule;
import org.apache.asterix.optimizer.rules.LoadRecordFieldsRule;
import org.apache.asterix.optimizer.rules.MetaFunctionToMetaVariableRule;
import org.apache.asterix.optimizer.rules.NestGroupByRule;
import org.apache.asterix.optimizer.rules.PushAggFuncIntoStandaloneAggregateRule;
import org.apache.asterix.optimizer.rules.PushAggregateIntoNestedSubplanRule;
import org.apache.asterix.optimizer.rules.PushFieldAccessRule;
import org.apache.asterix.optimizer.rules.PushGroupByThroughProduct;
import org.apache.asterix.optimizer.rules.PushLimitIntoOrderByRule;
import org.apache.asterix.optimizer.rules.PushLimitIntoPrimarySearchRule;
import org.apache.asterix.optimizer.rules.PushProperJoinThroughProduct;
import org.apache.asterix.optimizer.rules.PushSimilarityFunctionsBelowJoin;
import org.apache.asterix.optimizer.rules.RemoveDuplicateFieldsRule;
import org.apache.asterix.optimizer.rules.RemoveLeftOuterUnnestForLeftOuterJoinRule;
import org.apache.asterix.optimizer.rules.RemoveRedundantListifyRule;
import org.apache.asterix.optimizer.rules.RemoveRedundantSelectRule;
import org.apache.asterix.optimizer.rules.RemoveSortInFeedIngestionRule;
import org.apache.asterix.optimizer.rules.RemoveUnusedOneToOneEquiJoinRule;
import org.apache.asterix.optimizer.rules.RewriteDistinctAggregateRule;
import org.apache.asterix.optimizer.rules.SetAsterixMemoryRequirementsRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.asterix.optimizer.rules.SetClosedRecordConstructorsRule;
import org.apache.asterix.optimizer.rules.SetupCommitExtensionOpRule;
import org.apache.asterix.optimizer.rules.SimilarityCheckRule;
import org.apache.asterix.optimizer.rules.SweepIllegalNonfunctionalFunctions;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.asterix.optimizer.rules.am.IntroduceLSMComponentFilterRule;
import org.apache.asterix.optimizer.rules.am.IntroducePrimaryIndexForAggregationRule;
import org.apache.asterix.optimizer.rules.am.IntroduceSelectAccessMethodRule;
import org.apache.asterix.optimizer.rules.subplan.AsterixMoveFreeVariableOperatorOutOfSubplanRule;
import org.apache.asterix.optimizer.rules.subplan.InlineSubplanInputForNestedTupleSourceRule;
import org.apache.asterix.optimizer.rules.temporal.TranslateIntervalExpressionRule;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.CopyLimitDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateGroupByEmptyKeyRule;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceOrderByAfterSubplan;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractGroupByDecorVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import org.apache.hyracks.algebricks.rewriter.rules.HybridToInMemoryHashJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.InferTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineAssignIntoAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineSingleReferenceVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InsertProjectBeforeUnionRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroJoinInsideSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import org.apache.hyracks.algebricks.rewriter.rules.PushGroupByIntoSortRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushMapOperatorDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushNestedOrderByUnderPreSortedGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSortDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanWithAggregateDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushUnnestDownThroughUnionRule;
import org.apache.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveCartesianProductWithEmptyBranchRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantGroupByDecorVarsRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantWindowOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnnecessarySortMergeExchange;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.ReuseWindowAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;
import org.apache.hyracks.algebricks.rewriter.rules.SimpleUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.SwitchInnerJoinBranchRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.EliminateIsomorphicSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.EliminateSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.EliminateSubplanWithInputCardinalityOneRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.NestedSubplanToJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.PushSubplanIntoGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.SubplanOutOfGroupRule;

public final class RuleCollections {

    private RuleCollections() {
    }

    public static List<IAlgebraicRewriteRule> buildInitialTranslationRuleCollection() {
        List<IAlgebraicRewriteRule> translationRules = new LinkedList<>();
        translationRules.add(new TranslateIntervalExpressionRule());
        translationRules.add(new ExtractGroupByDecorVariablesRule());
        translationRules.add(new RemoveDuplicateFieldsRule());
        return translationRules;
    }

    public static final List<IAlgebraicRewriteRule> buildTypeInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<>();
        typeInfer.add(new InlineUnnestFunctionRule());
        typeInfer.add(new InferTypesRule());
        typeInfer.add(new CheckFilterExpressionTypeRule());
        return typeInfer;
    }

    public static final List<IAlgebraicRewriteRule> buildAutogenerateIDRuleCollection() {
        List<IAlgebraicRewriteRule> autogen = new LinkedList<>();
        autogen.add(new IntroduceAutogenerateIDRule());
        return autogen;
    }

    public static final List<IAlgebraicRewriteRule> buildFulltextContainsRuleCollection() {
        return Collections.singletonList(new FullTextContainsParameterCheckRule());
    }

    public static final List<IAlgebraicRewriteRule> buildNormalizationRuleCollection(ICcApplicationContext appCtx) {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<>();
        normalization.add(new CheckInsertUpsertReturningRule());
        normalization.add(new IntroduceUnnestForCollectionToSequenceRule());
        normalization.add(new EliminateSubplanRule());
        normalization.add(new EnforceOrderByAfterSubplan());
        normalization.add(new BreakSelectIntoConjunctsRule());
        normalization.add(new ExtractGbyExpressionsRule());
        normalization.add(new ExtractDistinctByExpressionsRule());
        normalization.add(new ExtractOrderExpressionsRule());
        normalization.add(new ExtractWindowExpressionsRule());

        // IntroduceStaticTypeCastRule should go before
        // IntroduceDynamicTypeCastRule to
        // avoid unnecessary dynamic casting
        normalization.add(new IntroduceStaticTypeCastForInsertRule());
        normalization.add(new IntroduceDynamicTypeCastRule());
        normalization.add(new IntroduceDynamicTypeCastForExternalFunctionRule());
        normalization.add(new IntroduceEnforcedListTypeRule());
        // Perform constant folding before common expression extraction
        normalization.add(new ConstantFoldingRule(appCtx));
        normalization.add(new ExtractCommonExpressionsRule());

        // Let PushAggFuncIntoStandaloneAggregateRule run after ExtractCommonExpressionsRule
        // so that PushAggFunc can happen in fewer places.
        normalization.add(new PushAggFuncIntoStandaloneAggregateRule());
        normalization.add(new ListifyUnnestingFunctionRule());
        normalization.add(new RemoveRedundantSelectRule());
        normalization.add(new CancelUnnestSingletonListRule());
        normalization.add(new UnnestToDataScanRule());
        normalization.add(new MetaFunctionToMetaVariableRule());
        normalization.add(new FuzzyEqRule());
        normalization.add(new SimilarityCheckRule());
        return normalization;
    }

    public static final List<IAlgebraicRewriteRule> buildCondPushDownAndJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> condPushDownAndJoinInference = new LinkedList<>();

        condPushDownAndJoinInference.add(new PushSelectDownRule());
        condPushDownAndJoinInference.add(new PushSortDownRule());
        condPushDownAndJoinInference.add(new RemoveRedundantListifyRule());
        condPushDownAndJoinInference.add(new CancelUnnestWithNestedListifyRule());
        condPushDownAndJoinInference.add(new SimpleUnnestToProductRule());
        condPushDownAndJoinInference.add(new ComplexUnnestToProductRule());
        condPushDownAndJoinInference.add(new DisjunctivePredicateToJoinRule());
        condPushDownAndJoinInference.add(new PushSelectIntoJoinRule());
        condPushDownAndJoinInference.add(new IntroJoinInsideSubplanRule());
        // Apply RemoveCartesianProductWithEmptyBranchRule before PushMapOperatorDownThroughProductRule
        // to avoid that a constant assignment gets pushed into an empty branch.
        condPushDownAndJoinInference.add(new RemoveCartesianProductWithEmptyBranchRule());
        condPushDownAndJoinInference.add(new PushMapOperatorDownThroughProductRule());
        condPushDownAndJoinInference.add(new PushSubplanWithAggregateDownThroughProductRule());
        condPushDownAndJoinInference.add(new SubplanOutOfGroupRule());
        // The following rule must run before PushAggregateIntoNestedSubplanRule
        // (before common subplans diverge due to aggregate pushdown)
        condPushDownAndJoinInference.add(new EliminateIsomorphicSubplanRule());

        condPushDownAndJoinInference.add(new AsterixExtractFunctionsFromJoinConditionRule());

        condPushDownAndJoinInference.add(new RemoveRedundantVariablesRule());
        condPushDownAndJoinInference.add(new AsterixInlineVariablesRule());
        condPushDownAndJoinInference.add(new RemoveUnusedAssignAndAggregateRule());

        condPushDownAndJoinInference.add(new FactorRedundantGroupAndDecorVarsRule());
        condPushDownAndJoinInference.add(new PushAggregateIntoNestedSubplanRule());
        condPushDownAndJoinInference.add(new EliminateSubplanRule());
        condPushDownAndJoinInference.add(new PushProperJoinThroughProduct());
        condPushDownAndJoinInference.add(new PushGroupByThroughProduct());
        condPushDownAndJoinInference.add(new NestGroupByRule());
        condPushDownAndJoinInference.add(new EliminateGroupByEmptyKeyRule());
        condPushDownAndJoinInference.add(new PushSubplanIntoGroupByRule());
        condPushDownAndJoinInference.add(new NestedSubplanToJoinRule());
        condPushDownAndJoinInference.add(new EliminateSubplanWithInputCardinalityOneRule());
        // The following rule should be fired after PushAggregateIntoNestedSubplanRule because
        // pulling invariants out of a subplan will make PushAggregateIntoGroupby harder.
        condPushDownAndJoinInference.add(new AsterixMoveFreeVariableOperatorOutOfSubplanRule());
        condPushDownAndJoinInference.add(new MetaFunctionToMetaVariableRule());
        return condPushDownAndJoinInference;
    }

    public static final List<IAlgebraicRewriteRule> buildLoadFieldsRuleCollection(ICcApplicationContext appCtx) {
        List<IAlgebraicRewriteRule> fieldLoads = new LinkedList<>();
        fieldLoads.add(new LoadRecordFieldsRule());
        fieldLoads.add(new PushFieldAccessRule());
        // fieldLoads.add(new ByNameToByHandleFieldAccessRule()); -- disabled
        fieldLoads.add(new ReinferAllTypesRule());
        fieldLoads.add(new ByNameToByIndexFieldAccessRule());
        fieldLoads.add(new RemoveRedundantVariablesRule());
        fieldLoads.add(new AsterixInlineVariablesRule());
        fieldLoads.add(new RemoveUnusedAssignAndAggregateRule());
        fieldLoads.add(new ConstantFoldingRule(appCtx));
        fieldLoads.add(new RemoveRedundantSelectRule());
        fieldLoads.add(new FeedScanCollectionToUnnest());
        fieldLoads.add(new NestedSubplanToJoinRule());
        fieldLoads.add(new InlineSubplanInputForNestedTupleSourceRule());
        fieldLoads.add(new RemoveLeftOuterUnnestForLeftOuterJoinRule());
        return fieldLoads;
    }

    public static final List<IAlgebraicRewriteRule> buildFuzzyJoinRuleCollection() {
        List<IAlgebraicRewriteRule> fuzzy = new LinkedList<>();
        fuzzy.add(new FuzzyJoinRule());
        fuzzy.add(new ExtractCommonExpressionsRule());
        fuzzy.add(new NestedSubplanToJoinRule());
        fuzzy.add(new PushSelectIntoJoinRule());
        fuzzy.add(new RemoveUnusedAssignAndAggregateRule());
        fuzzy.add(new InlineSubplanInputForNestedTupleSourceRule());
        fuzzy.add(new RemoveRedundantVariablesRule());
        fuzzy.add(new AsterixInlineVariablesRule());
        fuzzy.add(new RemoveUnusedAssignAndAggregateRule());
        return fuzzy;
    }

    public static final List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> consolidation = new LinkedList<>();
        consolidation.add(new ConsolidateSelectsRule());
        consolidation.add(new ConsolidateAssignsRule());
        consolidation.add(new InlineAssignIntoAggregateRule());
        consolidation.add(new RewriteDistinctAggregateRule());
        // The following rule should run after RewriteDistinctAggregateRule
        consolidation.add(new AsterixIntroduceGroupByCombinerRule());
        consolidation.add(new IntroduceAggregateCombinerRule());
        // Re-infer all types after introducing aggregate combiners
        consolidation.add(new ReinferAllTypesRule());
        consolidation.add(new CountVarToCountOneRule());
        consolidation.add(new RemoveUnusedAssignAndAggregateRule());
        consolidation.add(new RemoveRedundantGroupByDecorVarsRule());
        //PushUnnestDownUnion => RemoveRedundantListifyRule cause these rules are correlated
        consolidation.add(new PushUnnestDownThroughUnionRule());
        consolidation.add(new RemoveRedundantListifyRule());
        // Window operator consolidation rules
        consolidation.add(new AsterixConsolidateWindowOperatorsRule());
        consolidation.add(new ReuseWindowAggregateRule());
        consolidation.add(new RemoveRedundantWindowOperatorsRule());
        consolidation.add(new RemoveRedundantVariablesRule());
        return consolidation;
    }

    public static final List<IAlgebraicRewriteRule> buildAccessMethodRuleCollection() {
        List<IAlgebraicRewriteRule> accessMethod = new LinkedList<>();
        accessMethod.add(new IntroduceSelectAccessMethodRule());
        accessMethod.add(new IntroduceJoinAccessMethodRule());
        accessMethod.add(new IntroduceLSMComponentFilterRule());
        accessMethod.add(new IntroducePrimaryIndexForAggregationRule());
        accessMethod.add(new IntroduceSecondaryIndexInsertDeleteRule());
        accessMethod.add(new RemoveUnusedOneToOneEquiJoinRule());
        accessMethod.add(new PushSimilarityFunctionsBelowJoin());
        accessMethod.add(new RemoveUnusedAssignAndAggregateRule());
        return accessMethod;
    }

    public static final List<IAlgebraicRewriteRule> buildPlanCleanupRuleCollection() {
        List<IAlgebraicRewriteRule> planCleanupRules = new LinkedList<>();
        planCleanupRules.add(new SwitchInnerJoinBranchRule());
        planCleanupRules.add(new AsterixPushAssignBelowUnionAllRule());
        planCleanupRules.add(new ExtractCommonExpressionsRule());
        planCleanupRules.add(new RemoveRedundantVariablesRule());
        planCleanupRules.add(new PushProjectDownRule());
        planCleanupRules.add(new PushSelectDownRule());
        planCleanupRules.add(new SetClosedRecordConstructorsRule());
        planCleanupRules.add(new IntroduceDynamicTypeCastRule());
        planCleanupRules.add(new IntroduceDynamicTypeCastForExternalFunctionRule());
        planCleanupRules.add(new RemoveUnusedAssignAndAggregateRule());
        planCleanupRules.add(new RemoveCartesianProductWithEmptyBranchRule());
        planCleanupRules.add(new InjectTypeCastForFunctionArgumentsRule());
        planCleanupRules.add(new InjectTypeCastForUnionRule());

        // Needs to invoke ByNameToByIndexFieldAccessRule as the last logical optimization rule because
        // some rules can push a FieldAccessByName to a place where the name it tries to access is in the closed part.
        // For example, a possible scenario is that a field-access-by-name can be pushed down through UnionAllOperator.
        planCleanupRules.add(new ByNameToByIndexFieldAccessRule());
        return planCleanupRules;
    }

    public static final List<IAlgebraicRewriteRule> buildDataExchangeRuleCollection() {
        List<IAlgebraicRewriteRule> dataExchange = new LinkedList<>();
        dataExchange.add(new SetExecutionModeRule());
        dataExchange.add(new FindDataSourcesRule());
        return dataExchange;
    }

    public static final List<IAlgebraicRewriteRule> buildPhysicalRewritesAllLevelsRuleCollection() {
        List<IAlgebraicRewriteRule> physicalRewritesAllLevels = new LinkedList<>();
        physicalRewritesAllLevels.add(new PullSelectOutOfEqJoin());
        //Turned off the following rule for now not to change OptimizerTest results.
        physicalRewritesAllLevels.add(new SetupCommitExtensionOpRule());
        physicalRewritesAllLevels.add(new SetAsterixPhysicalOperatorsRule());
        physicalRewritesAllLevels.add(new SetAsterixMemoryRequirementsRule());
        // must run after SetMemoryRequirementsRule
        physicalRewritesAllLevels.add(new HybridToInMemoryHashJoinRule());
        physicalRewritesAllLevels.add(new AddEquivalenceClassForRecordConstructorRule());
        physicalRewritesAllLevels.add(new CheckFullParallelSortRule());
        physicalRewritesAllLevels.add(new EnforceStructuralPropertiesRule(BuiltinFunctions.RANGE_MAP,
                BuiltinFunctions.LOCAL_SAMPLING, BuiltinFunctions.NULL_WRITER));
        physicalRewritesAllLevels.add(new RemoveSortInFeedIngestionRule());
        physicalRewritesAllLevels.add(new RemoveUnnecessarySortMergeExchange());
        physicalRewritesAllLevels.add(new PushProjectDownRule());
        physicalRewritesAllLevels.add(new IntroduceMaterializationForInsertWithSelfScanRule());
        physicalRewritesAllLevels.add(new InlineSingleReferenceVariablesRule());
        physicalRewritesAllLevels.add(new RemoveUnusedAssignAndAggregateRule());
        physicalRewritesAllLevels.add(new ConsolidateAssignsRule());
        // After adding projects, we may need need to set physical operators again.
        physicalRewritesAllLevels.add(new SetAsterixPhysicalOperatorsRule());
        return physicalRewritesAllLevels;
    }

    public static final List<IAlgebraicRewriteRule> buildPhysicalRewritesTopLevelRuleCollection(
            ICcApplicationContext appCtx) {
        List<IAlgebraicRewriteRule> physicalRewritesTopLevel = new LinkedList<>();
        physicalRewritesTopLevel.add(new PushNestedOrderByUnderPreSortedGroupByRule());
        physicalRewritesTopLevel.add(new CopyLimitDownRule());
        // CopyLimitDownRule may generates non-topmost limits with numeric_adds functions.
        // We are going to apply a constant folding rule again for this case.
        physicalRewritesTopLevel.add(new ConstantFoldingRule(appCtx));
        physicalRewritesTopLevel.add(new PushLimitIntoOrderByRule());
        physicalRewritesTopLevel.add(new PushLimitIntoPrimarySearchRule());
        // remove assigns that could become unused after PushLimitIntoPrimarySearchRule
        physicalRewritesTopLevel.add(new RemoveUnusedAssignAndAggregateRule());
        physicalRewritesTopLevel.add(new IntroduceProjectsRule());
        physicalRewritesTopLevel.add(new SetAsterixPhysicalOperatorsRule());
        physicalRewritesTopLevel.add(new IntroduceRapidFrameFlushProjectAssignRule());
        physicalRewritesTopLevel.add(new SetExecutionModeRule());
        physicalRewritesTopLevel.add(new IntroduceRandomPartitioningFeedComputationRule());
        return physicalRewritesTopLevel;
    }

    public static final List<IAlgebraicRewriteRule> prepareForJobGenRuleCollection() {
        List<IAlgebraicRewriteRule> prepareForJobGenRewrites = new LinkedList<>();
        prepareForJobGenRewrites.add(new InsertProjectBeforeUnionRule());
        prepareForJobGenRewrites.add(new SetAsterixPhysicalOperatorsRule());
        prepareForJobGenRewrites
                .add(new IsolateHyracksOperatorsRule(HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
        prepareForJobGenRewrites.add(new FixReplicateOperatorOutputsRule());
        prepareForJobGenRewrites.add(new ExtractCommonOperatorsRule());
        // Re-infer all types, so that, e.g., the effect of not-is-null is propagated
        prepareForJobGenRewrites.add(new ReinferAllTypesRule());
        prepareForJobGenRewrites.add(new PushGroupByIntoSortRule());
        prepareForJobGenRewrites.add(new SetExecutionModeRule());
        prepareForJobGenRewrites.add(new SetAsterixMemoryRequirementsRule());
        prepareForJobGenRewrites.add(new SweepIllegalNonfunctionalFunctions());
        prepareForJobGenRewrites.add(new FixReplicateOperatorOutputsRule());
        return prepareForJobGenRewrites;
    }
}
