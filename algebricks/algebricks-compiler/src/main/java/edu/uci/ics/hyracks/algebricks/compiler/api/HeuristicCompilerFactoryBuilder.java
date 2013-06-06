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
package edu.uci.ics.hyracks.algebricks.compiler.api;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class HeuristicCompilerFactoryBuilder extends AbstractCompilerFactoryBuilder {

    public static class DefaultOptimizationContextFactory implements IOptimizationContextFactory {

        public static final DefaultOptimizationContextFactory INSTANCE = new DefaultOptimizationContextFactory();

        private DefaultOptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                PhysicalOptimizationConfig physicalOptimizationConfig) {
            return new AlgebricksOptimizationContext(varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                    physicalOptimizationConfig);
        }
    }

    private IOptimizationContextFactory optCtxFactory;

    public HeuristicCompilerFactoryBuilder() {
        this.optCtxFactory = DefaultOptimizationContextFactory.INSTANCE;
    }

    public HeuristicCompilerFactoryBuilder(IOptimizationContextFactory optCtxFactory) {
        this.optCtxFactory = optCtxFactory;
    }

    @Override
    public ICompilerFactory create() {
        return new ICompilerFactory() {
            @Override
            public ICompiler createCompiler(final ILogicalPlan plan, final IMetadataProvider<?, ?> metadata,
                    int varCounter) {
                final IOptimizationContext oc = optCtxFactory.createOptimizationContext(varCounter,
                        expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                        nullableTypeComputer, physicalOptimizationConfig);
                oc.setMetadataDeclarations(metadata);
                final HeuristicOptimizer opt = new HeuristicOptimizer(plan, logicalRewrites, physicalRewrites, oc);
                return new ICompiler() {

                    @Override
                    public void optimize() throws AlgebricksException {
                        opt.optimize();
                    }

                    @Override
                    public JobSpecification createJob(Object appContext,
                            IJobletEventListenerFactory jobEventListenerFactory) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting Job Generation.\n");
                        JobGenContext context = new JobGenContext(null, metadata, appContext,
                                serializerDeserializerProvider, hashFunctionFactoryProvider,
                                hashFunctionFamilyProvider, comparatorFactoryProvider, typeTraitProvider,
                                binaryBooleanInspectorFactory, binaryIntegerInspectorFactory, printerProvider,
                                nullWriterFactory, normalizedKeyComputerFactoryProvider, expressionRuntimeProvider,
                                expressionTypeComputer, nullableTypeComputer, oc, expressionEvalSizeComputer,
                                partialAggregationTypeComputer, predEvaluatorFactoryProvider,
                                physicalOptimizationConfig.getFrameSize(), clusterLocations);

                        PlanCompiler pc = new PlanCompiler(context);
                        return pc.compilePlan(plan, null, jobEventListenerFactory);
                    }
                };
            }
        };
    }

}
