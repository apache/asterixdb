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
package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime;

import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.examples.piglet.compiler.ConstantValue;
import edu.uci.ics.hyracks.algebricks.examples.piglet.exceptions.PigletException;
import edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions.PigletFunctionRegistry;
import edu.uci.ics.hyracks.algebricks.examples.piglet.types.Type;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class PigletExpressionJobGen implements ILogicalExpressionJobGen {
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
                            UTF8StringSerializerDeserializer.INSTANCE.serialize(image, dos);
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