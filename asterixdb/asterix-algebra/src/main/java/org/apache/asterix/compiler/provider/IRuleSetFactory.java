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

import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;

public interface IRuleSetFactory {

    enum RuleSetKind implements IRuleSetKind {
        QUERY,
        LOGICAL_ADVISOR,
        SAMPLING,
        /**
         * Samples a copy of a subplan that has already been through the logical rewrites, to compute a range map
         * at compile time. Unlike {@link #SAMPLING}, it does not introduce aggregate combiners: the subplan already
         * has them, and splitting an aggregate twice feeds one local step's output into another.
         */
        RANGE_MAP_SAMPLING
    }

    /**
     * @return the logical rewrites
     */
    List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(ICcApplicationContext appCtx);

    /**
     * @return the logical rewrites of the specified kind
     */
    List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx);

    /**
     * @return the physical rewrites
     */
    List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(ICcApplicationContext appCtx);

    /**
     * @return the physical rewrites of the specified kind
     */
    List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx);

}
