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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
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
                IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                PhysicalOptimizationConfig physicalOptimizationConfig) {
            LogicalOperatorPrettyPrintVisitor prettyPrintVisitor = new LogicalOperatorPrettyPrintVisitor();
            return new AlgebricksOptimizationContext(varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                    physicalOptimizationConfig, prettyPrintVisitor);
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
