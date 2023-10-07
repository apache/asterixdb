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
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.lang.common.base.IAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.lang.sqlpp.visitor.SqlppAstPrintVisitorFactory;
import org.apache.asterix.optimizer.base.FuzzyUtils;
import org.apache.asterix.optimizer.rules.DisjunctivePredicateToJoinRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.asterix.optimizer.rules.cbo.JoinEnum;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslatorFactory;

public class SqlppCompilationProvider implements ILangCompilationProvider {

    protected final INamespaceResolver namespaceResolver;

    public SqlppCompilationProvider(INamespaceResolver namespaceResolver) {
        this.namespaceResolver = namespaceResolver;
    }

    @Override
    public ILangExtension.Language getLanguage() {
        return ILangExtension.Language.SQLPP;
    }

    @Override
    public IParserFactory getParserFactory() {
        return new SqlppParserFactory(namespaceResolver);
    }

    @Override
    public IRewriterFactory getRewriterFactory() {
        return new SqlppRewriterFactory(getParserFactory());
    }

    @Override
    public IAstPrintVisitorFactory getAstPrintVisitorFactory() {
        return new SqlppAstPrintVisitorFactory();
    }

    @Override
    public ILangExpressionToPlanTranslatorFactory getExpressionToPlanTranslatorFactory() {
        return new SqlppExpressionToPlanTranslatorFactory();
    }

    @Override
    public IRuleSetFactory getRuleSetFactory() {
        return new DefaultRuleSetFactory();
    }

    @Override
    public Set<String> getCompilerOptions() {
        return new HashSet<>(Set.of(CompilerProperties.COMPILER_JOINMEMORY_KEY,
                CompilerProperties.COMPILER_GROUPMEMORY_KEY, CompilerProperties.COMPILER_SORTMEMORY_KEY,
                CompilerProperties.COMPILER_WINDOWMEMORY_KEY, CompilerProperties.COMPILER_TEXTSEARCHMEMORY_KEY,
                CompilerProperties.COMPILER_PARALLELISM_KEY, CompilerProperties.COMPILER_SORT_PARALLEL_KEY,
                CompilerProperties.COMPILER_SORT_SAMPLES_KEY, CompilerProperties.COMPILER_EXTERNALSCANMEMORY_KEY,
                CompilerProperties.COMPILER_INDEXONLY_KEY, CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY,
                CompilerProperties.COMPILER_EXTERNAL_FIELD_PUSHDOWN_KEY, CompilerProperties.COMPILER_SUBPLAN_MERGE_KEY,
                CompilerProperties.COMPILER_SUBPLAN_NESTEDPUSHDOWN_KEY, CompilerProperties.COMPILER_ARRAYINDEX_KEY,
                CompilerProperties.COMPILER_CBO_KEY, CompilerProperties.COMPILER_CBO_TEST_KEY,
                CompilerProperties.COMPILER_FORCE_JOIN_ORDER_KEY, CompilerProperties.COMPILER_QUERY_PLAN_SHAPE_KEY,
                CompilerProperties.COMPILER_MIN_MEMORY_ALLOCATION_KEY, CompilerProperties.COMPILER_COLUMN_FILTER_KEY,
                CompilerProperties.COMPILER_BATCH_LOOKUP_KEY, FunctionUtil.IMPORT_PRIVATE_FUNCTIONS,
                FuzzyUtils.SIM_FUNCTION_PROP_NAME, FuzzyUtils.SIM_THRESHOLD_PROP_NAME,
                StartFeedStatement.WAIT_FOR_COMPLETION, FeedActivityDetails.FEED_POLICY_NAME,
                FeedActivityDetails.COLLECT_LOCATIONS, SqlppQueryRewriter.INLINE_WITH_OPTION,
                SqlppExpressionToPlanTranslator.REWRITE_IN_AS_OR_OPTION, "hash_merge", "output-record-type",
                DisjunctivePredicateToJoinRule.REWRITE_OR_AS_JOIN_OPTION,
                SetAsterixPhysicalOperatorsRule.REWRITE_ATTEMPT_BATCH_ASSIGN,
                EquivalenceClassUtils.REWRITE_INTERNAL_QUERYUID_PK, SqlppQueryRewriter.SQL_COMPAT_OPTION,
                JoinEnum.CBO_FULL_ENUM_LEVEL_KEY, JoinEnum.CBO_CP_ENUM_KEY));
    }
}
