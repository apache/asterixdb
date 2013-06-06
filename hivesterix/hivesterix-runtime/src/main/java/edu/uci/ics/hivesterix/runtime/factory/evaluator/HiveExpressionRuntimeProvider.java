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
package edu.uci.ics.hivesterix.runtime.factory.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.uci.ics.hivesterix.logical.expression.ExpressionConstant;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter.AggregateFunctionFactoryAdapter;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter.ScalarEvaluatorFactoryAdapter;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter.UnnestingFunctionFactoryAdapter;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;

public class HiveExpressionRuntimeProvider implements IExpressionRuntimeProvider {

    public static final IExpressionRuntimeProvider INSTANCE = new HiveExpressionRuntimeProvider();

    @Override
    public IAggregateEvaluatorFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new AggregateFunctionFactoryAdapter(new AggregationFunctionFactory(expr, schema, env));
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new AggregationFunctionSerializableFactory(expr, schema, env);
    }

    @Override
    public IRunningAggregateEvaluatorFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new UnnestingFunctionFactoryAdapter(new UnnestingFunctionFactory(expr, schema, env));
    }

    public IScalarEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                return new ScalarEvaluatorFactoryAdapter(createVariableEvaluatorFactory(v, env, inputSchemas, context));
            }
            case CONSTANT: {
                ConstantExpression c = (ConstantExpression) expr;
                return new ScalarEvaluatorFactoryAdapter(createConstantEvaluatorFactory(c, env, inputSchemas, context));
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier fid = fun.getFunctionIdentifier();

                if (fid.getName().equals(ExpressionConstant.FIELDACCESS)) {
                    return new ScalarEvaluatorFactoryAdapter(createFieldExpressionEvaluatorFactory(fun, env,
                            inputSchemas, context));
                }

                if (fid.getName().equals(ExpressionConstant.FIELDACCESS)) {
                    return new ScalarEvaluatorFactoryAdapter(createNullExpressionEvaluatorFactory(fun, env,
                            inputSchemas, context));
                }

                if (fun.getKind() == FunctionKind.SCALAR) {
                    ScalarFunctionCallExpression scalar = (ScalarFunctionCallExpression) fun;
                    return new ScalarEvaluatorFactoryAdapter(createScalarFunctionEvaluatorFactory(scalar, env,
                            inputSchemas, context));
                } else {
                    throw new AlgebricksException("Cannot create evaluator for function " + fun + " of kind "
                            + fun.getKind());
                }
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private ICopyEvaluatorFactory createVariableEvaluatorFactory(VariableReferenceExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new ColumnExpressionEvaluatorFactory(expr, schema, env);
    }

    private ICopyEvaluatorFactory createScalarFunctionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        List<String> names = new ArrayList<String>();
        List<TypeInfo> types = new ArrayList<TypeInfo>();
        for (IOperatorSchema inputSchema : inputSchemas) {
            Schema schema = this.getSchema(inputSchema, env);
            names.addAll(schema.getNames());
            types.addAll(schema.getTypes());
        }
        Schema inputSchema = new Schema(names, types);
        return new ScalarFunctionExpressionEvaluatorFactory(expr, inputSchema, env);
    }

    private ICopyEvaluatorFactory createFieldExpressionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new FieldExpressionEvaluatorFactory(expr, schema, env);
    }

    private ICopyEvaluatorFactory createNullExpressionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new NullExpressionEvaluatorFactory(expr, schema, env);
    }

    private ICopyEvaluatorFactory createConstantEvaluatorFactory(ConstantExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        Schema schema = this.getSchema(inputSchemas[0], env);
        return new ConstantExpressionEvaluatorFactory(expr, schema, env);
    }

    private Schema getSchema(IOperatorSchema inputSchema, IVariableTypeEnvironment env) throws AlgebricksException {
        List<String> names = new ArrayList<String>();
        List<TypeInfo> types = new ArrayList<TypeInfo>();
        Iterator<LogicalVariable> variables = inputSchema.iterator();
        while (variables.hasNext()) {
            LogicalVariable var = variables.next();
            names.add(var.toString());
            types.add((TypeInfo) env.getVarType(var));
        }

        Schema schema = new Schema(names, types);
        return schema;
    }

}