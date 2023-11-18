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
package org.apache.hyracks.algebricks.compiler.api;

import java.util.EnumSet;
import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.runtime.writers.SerializedDataWriterFactory;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobSpecification;

public class HeuristicCompilerFactoryBuilder extends AbstractCompilerFactoryBuilder {

    public static class DefaultOptimizationContextFactory implements IOptimizationContextFactory {

        public static final DefaultOptimizationContextFactory INSTANCE = new DefaultOptimizationContextFactory();

        private DefaultOptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer missableTypeComputer,
                IConflictingTypeResolver conflictingTypeResolver, PhysicalOptimizationConfig physicalOptimizationConfig,
                AlgebricksPartitionConstraint clusterLocations, IWarningCollector warningCollector) {
            IPlanPrettyPrinter prettyPrinter = PlanPrettyPrinter.createStringPlanPrettyPrinter();
            return new AlgebricksOptimizationContext(this, varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, missableTypeComputer,
                    conflictingTypeResolver, physicalOptimizationConfig, clusterLocations, prettyPrinter,
                    warningCollector);
        }

        @Override
        public IOptimizationContext cloneOptimizationContext(IOptimizationContext oc) {
            return new AlgebricksOptimizationContext((AlgebricksOptimizationContext) oc);
        }
    }

    private final IOptimizationContextFactory optCtxFactory;

    public HeuristicCompilerFactoryBuilder() {
        this.optCtxFactory = DefaultOptimizationContextFactory.INSTANCE;
    }

    public HeuristicCompilerFactoryBuilder(IOptimizationContextFactory optCtxFactory) {
        this.optCtxFactory = optCtxFactory;
    }

    @Override
    public ICompilerFactory create() {
        return new CompilerFactoryImpl();
    }

    private class CompilerFactoryImpl implements ICompilerFactory {
        @Override
        public ICompiler createCompiler(ILogicalPlan plan, IMetadataProvider<?, ?> metadata, int varCounter) {
            IOptimizationContext optContext =
                    optCtxFactory.createOptimizationContext(varCounter, expressionEvalSizeComputer,
                            mergeAggregationExpressionFactory, expressionTypeComputer, missableTypeComputer,
                            conflictingTypeResolver, physicalOptimizationConfig, clusterLocations, warningCollector);
            optContext.setMetadataDeclarations(metadata);
            optContext.setCompilerFactory(this);
            return new CompilerImpl(this, plan, optContext, logicalRewrites.get(), physicalRewrites.get(),
                    writerFactory);
        }

        @Override
        public ICompiler createCompiler(ILogicalPlan plan, IOptimizationContext newOptContext,
                IRuleSetKind ruleSetKind) {
            if (newOptContext.getCompilerFactory() != this) {
                throw new IllegalStateException();
            }
            return new CompilerImpl(this, plan, newOptContext, logicalRewritesByKind.apply(ruleSetKind),
                    physicalRewrites.get(), SerializedDataWriterFactory.WITHOUT_RECORD_DESCRIPTOR);
        }

        private PlanCompiler createPlanCompiler(IOptimizationContext oc, Object appContext,
                IAWriterFactory writerFactory) {
            JobGenContext context = new JobGenContext(null, oc.getMetadataProvider(), appContext,
                    serializerDeserializerProvider, hashFunctionFactoryProvider, hashFunctionFamilyProvider,
                    comparatorFactoryProvider, typeTraitProvider, binaryBooleanInspectorFactory,
                    binaryIntegerInspectorFactory, printerProvider, writerFactory, resultSerializerFactoryProvider,
                    missingWriterFactory, nullWriterFactory, unnestingPositionWriterFactory,
                    normalizedKeyComputerFactoryProvider, expressionRuntimeProvider, expressionTypeComputer, oc,
                    expressionEvalSizeComputer, partialAggregationTypeComputer, predEvaluatorFactoryProvider,
                    physicalOptimizationConfig.getFrameSize(), clusterLocations, warningCollector, maxWarnings,
                    physicalOptimizationConfig);
            return new PlanCompiler(context);
        }
    }

    private static class CompilerImpl implements ICompiler {

        private final CompilerFactoryImpl factory;

        private final ILogicalPlan plan;

        private final IOptimizationContext oc;

        private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;

        private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;

        private final IAWriterFactory writerFactory;

        private CompilerImpl(CompilerFactoryImpl factory, ILogicalPlan plan, IOptimizationContext oc,
                List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites,
                List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites,
                IAWriterFactory writerFactory) {
            this.factory = factory;
            this.plan = plan;
            this.oc = oc;
            this.logicalRewrites = logicalRewrites;
            this.physicalRewrites = physicalRewrites;
            this.writerFactory = writerFactory;
        }

        @Override
        public void optimize() throws AlgebricksException {
            HeuristicOptimizer opt = new HeuristicOptimizer(plan, logicalRewrites, physicalRewrites, oc);
            opt.optimize();
        }

        @Override
        public JobSpecification createJob(Object appContext, IJobletEventListenerFactory jobEventListenerFactory)
                throws AlgebricksException {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
            PlanCompiler pc = factory.createPlanCompiler(oc, appContext, writerFactory);
            return pc.compilePlan(plan, jobEventListenerFactory);
        }

        @Override
        public JobSpecification createJob(Object appContext, IJobletEventListenerFactory jobEventListenerFactory,
                EnumSet<JobFlag> runtimeFlags) throws AlgebricksException {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
            PlanCompiler pc = factory.createPlanCompiler(oc, appContext, writerFactory);
            if (runtimeFlags.contains(JobFlag.PROFILE_RUNTIME)) {
                pc.enableLog2PhysMapping();
            }
            return pc.compilePlan(plan, jobEventListenerFactory);
        }

        @Override
        public boolean skipJobCapacityAssignment() {
            return oc.skipJobCapacityAssignment();
        }
    }
}
