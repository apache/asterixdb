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
package org.apache.hyracks.algebricks.examples.piglet.runtime;

import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.examples.piglet.compiler.ConstantValue;
import org.apache.hyracks.algebricks.examples.piglet.exceptions.PigletException;
import org.apache.hyracks.algebricks.examples.piglet.runtime.functions.PigletFunctionRegistry;
import org.apache.hyracks.algebricks.examples.piglet.types.Type;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class PigletExpressionJobGen implements ILogicalExpressionJobGen {
    private final UTF8StringSerializerDeserializer utf8SerDer = new UTF8StringSerializerDeserializer();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case CONSTANT: {
                ConstantValue cv = (ConstantValue) ((ConstantExpression) expr).getValue();
                Type type = cv.getType();
                String image = cv.getImage();
                ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                DataOutput dos = abvs.getDataOutput();
                switch (type.getTag()) {
                    case INTEGER:
                        try {
                            IntegerSerializerDeserializer.INSTANCE.serialize(Integer.valueOf(image), dos);
                        } catch (Exception e) {
                            throw new AlgebricksException(e);
                        }
                        break;

                    case CHAR_ARRAY:
                        try {
                            utf8SerDer.serialize(image, dos);
                        } catch (Exception e) {
                            throw new AlgebricksException(e);
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException("Unsupported constant type: " + type.getTag());
                }
                return new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
            }

            case FUNCTION_CALL: {
                ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) expr;

                List<Mutable<ILogicalExpression>> argExprs = sfce.getArguments();
                ICopyEvaluatorFactory argEvalFactories[] = new ICopyEvaluatorFactory[argExprs.size()];
                for (int i = 0; i < argEvalFactories.length; ++i) {
                    Mutable<ILogicalExpression> er = argExprs.get(i);
                    argEvalFactories[i] = createEvaluatorFactory(er.getValue(), env, inputSchemas, context);
                }
                ICopyEvaluatorFactory funcEvalFactory;
                try {
                    funcEvalFactory = PigletFunctionRegistry.createFunctionEvaluatorFactory(
                            sfce.getFunctionIdentifier(), argEvalFactories);
                } catch (PigletException e) {
                    throw new AlgebricksException(e);
                }
                return funcEvalFactory;
            }

            case VARIABLE: {
                LogicalVariable var = ((VariableReferenceExpression) expr).getVariableReference();
                int index = inputSchemas[0].findVariable(var);
                return new ColumnAccessEvalFactory(index);
            }
        }
        throw new IllegalArgumentException("Unknown expression type: " + expr.getExpressionTag());
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }
}