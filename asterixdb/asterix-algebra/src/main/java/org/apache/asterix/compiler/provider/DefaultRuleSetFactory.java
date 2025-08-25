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
package org.apache.asterix.compiler.provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.optimizer.cost.CostMethods;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFirstRuleCheckFixpointRuleController;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;

public class DefaultRuleSetFactory implements IRuleSetFactory {

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(
            ICcApplicationContext appCtx) {
        return buildLogical(appCtx);
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx) {
        if (ruleSetKind == RuleSetKind.SAMPLING) {
            return buildLogicalSampling();
        } else if (ruleSetKind == RuleSetKind.QUERY) {
            return getLogicalRewrites(appCtx);
        } else if (ruleSetKind == RuleSetKind.LOGICAL_ADVISOR) {
            return getLogicalRewrites(appCtx);
        } else {
            throw new IllegalArgumentException(String.valueOf(ruleSetKind));
        }
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(
            ICcApplicationContext appCtx) {
        return buildPhysical(appCtx, CostMethods::new);
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx) {
        if (ruleSetKind == RuleSetKind.QUERY) {
            return buildPhysical(appCtx, CostMethods::new);
        } else if (ruleSetKind == RuleSetKind.SAMPLING) {
            return buildPhysical(appCtx, CostMethods::new);
        } else {
            return Collections.emptyList();
        }
    }

    public static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildLogical(
            ICcApplicationContext appCtx) {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultLogicalRewrites = new ArrayList<>();
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(true);
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        SequentialFirstRuleCheckFixpointRuleController seqFirstRuleGateKeeperDfs =
                new SequentialFirstRuleCheckFixpointRuleController(true);
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildInitialTranslationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildTypeInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildAutogenerateIDRuleCollection()));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildNormalizationRuleCollection(appCtx)));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildCondPushDownAndJoinInferenceRuleCollection()));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildLoadFieldsRuleCollection(appCtx, false)));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildNormalizationRuleCollection(appCtx)));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildCondPushDownAndJoinInferenceRuleCollection()));
        defaultLogicalRewrites
                .add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildLoadFieldsRuleCollection(appCtx, true)));
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildFulltextContainsRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildDataExchangeRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildCBORuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildConsolidationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildAccessMethodRuleCollection()));
        defaultLogicalRewrites.add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildPlanCleanupRuleCollection()));

        //put TXnRuleCollection!
        return defaultLogicalRewrites;
    }

    public static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildLogicalSampling() {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites = new ArrayList<>();
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        logicalRewrites.add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildConsolidationRuleCollection()));
        logicalRewrites.add(new Pair<>(seqCtrlNoDfs, RuleCollections.buildPlanCleanupRuleCollection()));
        return logicalRewrites;
    }

    public static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildPhysical(
            ICcApplicationContext appCtx, SetAsterixPhysicalOperatorsRule.CostMethodsFactory cmf) {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultPhysicalRewrites = new ArrayList<>();
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        SequentialOnceRuleController seqOnceTopLevel = new SequentialOnceRuleController(false);
        defaultPhysicalRewrites
                .add(new Pair<>(seqOnceCtrl, RuleCollections.buildPhysicalRewritesAllLevelsRuleCollection(cmf)));
        defaultPhysicalRewrites.add(
                new Pair<>(seqOnceTopLevel, RuleCollections.buildPhysicalRewritesTopLevelRuleCollection(appCtx, cmf)));
        defaultPhysicalRewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.prepareForJobGenRuleCollection(cmf)));
        return defaultPhysicalRewrites;
    }
}
