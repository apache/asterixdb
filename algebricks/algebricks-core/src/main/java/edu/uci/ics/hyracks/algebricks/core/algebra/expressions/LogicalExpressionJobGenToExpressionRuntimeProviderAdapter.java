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
package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class LogicalExpressionJobGenToExpressionRuntimeProviderAdapter implements IExpressionRuntimeProvider {
    private final ILogicalExpressionJobGen lejg;

    public LogicalExpressionJobGenToExpressionRuntimeProviderAdapter(ILogicalExpressionJobGen lejg) {
        this.lejg = lejg;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        ICopyEvaluatorFactory cef = lejg.createEvaluatorFactory(expr, env, inputSchemas, context);
        return new ScalarEvaluatorFactoryAdapter(cef);
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyAggregateFunctionFactory caff = lejg.createAggregateFunctionFactory(expr, env, inputSchemas, context);
        return new AggregateFunctionFactoryAdapter(caff);
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        return lejg.createSerializableAggregateFunctionFactory(expr, env, inputSchemas, context);
    }

    @Override
    public IRunningAggregateEvaluatorFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyRunningAggregateFunctionFactory craff = lejg.createRunningAggregateFunctionFactory(expr, env,
                inputSchemas, context);
        return new RunningAggregateFunctionFactoryAdapter(craff);
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyUnnestingFunctionFactory cuff = lejg.createUnnestingFunctionFactory(expr, env, inputSchemas, context);
        return new UnnestingFunctionFactoryAdapter(cuff);
    }

    public static final class ScalarEvaluatorFactoryAdapter implements IScalarEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final ICopyEvaluatorFactory cef;

        public ScalarEvaluatorFactoryAdapter(ICopyEvaluatorFactory cef) {
            this.cef = cef;
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final ICopyEvaluator ce = cef.createEvaluator(abvs);
            return new IScalarEvaluator() {
                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                    abvs.reset();
                    ce.evaluate(tuple);
                    result.set(abvs);
                }
            };
        }
    }

    public static final class AggregateFunctionFactoryAdapter implements IAggregateEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final ICopyAggregateFunctionFactory caff;

        public AggregateFunctionFactoryAdapter(ICopyAggregateFunctionFactory caff) {
            this.caff = caff;
        }

        @Override
        public IAggregateEvaluator createAggregateEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final ICopyAggregateFunction caf = caff.createAggregateFunction(abvs);
            return new IAggregateEvaluator() {
                @Override
                public void step(IFrameTupleReference tuple) throws AlgebricksException {
                    caf.step(tuple);
                }

                @Override
                public void init() throws AlgebricksException {
                    abvs.reset();
                    caf.init();
                }

                @Override
                public void finish(IPointable result) throws AlgebricksException {
                    caf.finish();
                    result.set(abvs);
                }
            };
        }
    }

    public static final class RunningAggregateFunctionFactoryAdapter implements IRunningAggregateEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final ICopyRunningAggregateFunctionFactory craff;

        public RunningAggregateFunctionFactoryAdapter(ICopyRunningAggregateFunctionFactory craff) {
            this.craff = craff;
        }

        @Override
        public IRunningAggregateEvaluator createRunningAggregateEvaluator() throws AlgebricksException {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final ICopyRunningAggregateFunction craf = craff.createRunningAggregateFunction(abvs);
            return new IRunningAggregateEvaluator() {
                @Override
                public void step(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                    abvs.reset();
                    craf.step(tuple);
                    result.set(abvs);
                }

                @Override
                public void init() throws AlgebricksException {
                    craf.init();
                }
            };
        }
    }

    public static final class UnnestingFunctionFactoryAdapter implements IUnnestingEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final ICopyUnnestingFunctionFactory cuff;

        public UnnestingFunctionFactoryAdapter(ICopyUnnestingFunctionFactory cuff) {
            this.cuff = cuff;
        }

        @Override
        public IUnnestingEvaluator createUnnestingEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final ICopyUnnestingFunction cuf = cuff.createUnnestingFunction(abvs);
            return new IUnnestingEvaluator() {
                @Override
                public boolean step(IPointable result) throws AlgebricksException {
                    abvs.reset();
                    if (cuf.step()) {
                        result.set(abvs);
                        return true;
                    }
                    return false;
                }

                @Override
                public void init(IFrameTupleReference tuple) throws AlgebricksException {
                    cuf.init(tuple);
                }
            };
        }
    }
}