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

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.lang.common.base.IAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.optimizer.base.FuzzyUtils;
import org.apache.asterix.optimizer.rules.DisjunctivePredicateToJoinRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;

public interface ILangCompilationProvider {
    /**
     * @return language kind
     */
    ILangExtension.Language getLanguage();

    /**
     * @return the parser factory of a language implementation.
     */
    IParserFactory getParserFactory();

    /**
     * @return the rewriter factory of a language implementation.
     */
    IRewriterFactory getRewriterFactory();

    /**
     * @return the AST printer factory of a language implementation.
     */
    IAstPrintVisitorFactory getAstPrintVisitorFactory();

    /**
     * @return the language expression to logical query plan translator factory of a language implementation.
     */
    ILangExpressionToPlanTranslatorFactory getExpressionToPlanTranslatorFactory();

    /**
     * @return the rule set factory of a language implementation
     */
    IRuleSetFactory getRuleSetFactory();

    /**
     * @return all configurable parameters of a language implementation.
     */
    default Set<String> getConfigurableParameters() {
        return new HashSet<>(Set.of(CompilerProperties.COMPILER_JOINMEMORY_KEY,
                CompilerProperties.COMPILER_GROUPMEMORY_KEY, CompilerProperties.COMPILER_SORTMEMORY_KEY,
                CompilerProperties.COMPILER_WINDOWMEMORY_KEY, CompilerProperties.COMPILER_TEXTSEARCHMEMORY_KEY,
                CompilerProperties.COMPILER_PARALLELISM_KEY, CompilerProperties.COMPILER_SORT_PARALLEL_KEY,
                CompilerProperties.COMPILER_SORT_SAMPLES_KEY, CompilerProperties.COMPILER_EXTERNALSCANMEMORY_KEY,
                CompilerProperties.COMPILER_INDEXONLY_KEY, CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY,
                CompilerProperties.COMPILER_EXTERNAL_FIELD_PUSHDOWN_KEY, CompilerProperties.COMPILER_SUBPLAN_MERGE_KEY,
                CompilerProperties.COMPILER_SUBPLAN_NESTEDPUSHDOWN_KEY, CompilerProperties.COMPILER_ARRAYINDEX_KEY,
                CompilerProperties.COMPILER_MIN_MEMORY_ALLOCATION_KEY, FunctionUtil.IMPORT_PRIVATE_FUNCTIONS,
                FuzzyUtils.SIM_FUNCTION_PROP_NAME, FuzzyUtils.SIM_THRESHOLD_PROP_NAME,
                StartFeedStatement.WAIT_FOR_COMPLETION, FeedActivityDetails.FEED_POLICY_NAME,
                FeedActivityDetails.COLLECT_LOCATIONS, SqlppQueryRewriter.INLINE_WITH_OPTION,
                SqlppExpressionToPlanTranslator.REWRITE_IN_AS_OR_OPTION, "hash_merge", "output-record-type",
                DisjunctivePredicateToJoinRule.REWRITE_OR_AS_JOIN_OPTION,
                SetAsterixPhysicalOperatorsRule.REWRITE_ATTEMPT_BATCH_ASSIGN,
                EquivalenceClassUtils.REWRITE_INTERNAL_QUERYUID_PK, SqlppQueryRewriter.SQL_COMPAT_OPTION));
    }
}
