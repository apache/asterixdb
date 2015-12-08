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
package org.apache.asterix.jobgen;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.asterix.external.library.ExternalFunctionDescriptorProvider;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.ComparisonEvalFactory;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;

public class QueryLogicalExpressionJobGen implements ILogicalExpressionJobGen {

    public static final QueryLogicalExpressionJobGen INSTANCE = new QueryLogicalExpressionJobGen();

    private QueryLogicalExpressionJobGen() {
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
                    throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd = getFunctionDescriptor(expr, env, context);
        switch (fd.getFunctionDescriptorTag()) {
            case SERIALAGGREGATE:
                return null;
            case AGGREGATE:
                return fd.createAggregateFunctionFactory(args);
            default:
                throw new IllegalStateException(
                        "Invalid function descriptor " + fd.getFunctionDescriptorTag() + " expected "
                                + FunctionDescriptorTag.SERIALAGGREGATE + " or " + FunctionDescriptorTag.AGGREGATE);
        }
    }

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(
            StatefulFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        return getFunctionDescriptor(expr, env, context).createRunningAggregateFunctionFactory(args);
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
                    throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        return getFunctionDescriptor(expr, env, context).createUnnestingFunctionFactory(args);
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        ICopyEvaluatorFactory copyEvaluatorFactory = null;
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                copyEvaluatorFactory = createVariableEvaluatorFactory(v, inputSchemas, context);
                return copyEvaluatorFactory;
            }
            case CONSTANT: {
                ConstantExpression c = (ConstantExpression) expr;
                copyEvaluatorFactory = createConstantEvaluatorFactory(c, inputSchemas, context);
                return copyEvaluatorFactory;
            }
            case FUNCTION_CALL: {
                copyEvaluatorFactory = createScalarFunctionEvaluatorFactory((AbstractFunctionCallExpression) expr, env,
                        inputSchemas, context);
                return copyEvaluatorFactory;
            }
            default:
                throw new IllegalStateException();
        }

    }

    private ICopyEvaluatorFactory createVariableEvaluatorFactory(VariableReferenceExpression expr,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        LogicalVariable variable = expr.getVariableReference();
        for (IOperatorSchema scm : inputSchemas) {
            int pos = scm.findVariable(variable);
            if (pos >= 0) {
                return new ColumnAccessEvalFactory(pos);
            }
        }
        throw new AlgebricksException("Variable " + variable + " could not be found in any input schema.");
    }

    private ICopyEvaluatorFactory createScalarFunctionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
                    throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
        if (ck != null) {
            return new ComparisonEvalFactory(args[0], args[1], ck);
        }

        IFunctionDescriptor fd = null;
        if (!(expr.getFunctionInfo() instanceof IExternalFunctionInfo)) {
            IDataFormat format = FormatUtils.getDefaultFormat();
            fd = format.resolveFunction(expr, env);
        } else {
            try {
                fd = ExternalFunctionDescriptorProvider
                        .getExternalFunctionDescriptor((IExternalFunctionInfo) expr.getFunctionInfo());
            } catch (AsterixException ae) {
                throw new AlgebricksException(ae);
            }
        }
        return fd.createEvaluatorFactory(args);
    }

    private ICopyEvaluatorFactory createConstantEvaluatorFactory(ConstantExpression expr,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        IDataFormat format = FormatUtils.getDefaultFormat();
        return format.getConstantEvalFactory(expr.getValue());
    }

    private ICopyEvaluatorFactory[] codegenArguments(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> arguments = expr.getArguments();
        int n = arguments.size();
        ICopyEvaluatorFactory[] args = new ICopyEvaluatorFactory[n];
        int i = 0;
        for (Mutable<ILogicalExpression> a : arguments) {
            args[i++] = createEvaluatorFactory(a.getValue(), env, inputSchemas, context);
        }
        return args;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd = getFunctionDescriptor(expr, env, context);

        switch (fd.getFunctionDescriptorTag()) {
            case AGGREGATE: {
                if (AsterixBuiltinFunctions.isAggregateFunctionSerializable(fd.getIdentifier())) {
                    AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                            .makeSerializableAggregateFunctionExpression(fd.getIdentifier(), expr.getArguments());
                    IFunctionDescriptor afdd = getFunctionDescriptor(serialAggExpr, env, context);
                    return afdd.createSerializableAggregateFunctionFactory(args);
                } else {
                    throw new AlgebricksException(
                            "Trying to create a serializable aggregate from a non-serializable aggregate function descriptor. (fi="
                                    + expr.getFunctionIdentifier() + ")");
                }
            }
            case SERIALAGGREGATE: {
                return fd.createSerializableAggregateFunctionFactory(args);
            }

            default:
                throw new IllegalStateException(
                        "Invalid function descriptor " + fd.getFunctionDescriptorTag() + " expected "
                                + FunctionDescriptorTag.SERIALAGGREGATE + " or " + FunctionDescriptorTag.AGGREGATE);
        }
    }

    private IFunctionDescriptor getFunctionDescriptor(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            JobGenContext context) throws AlgebricksException {
        IFunctionDescriptor fd = FormatUtils.getDefaultFormat().resolveFunction(expr, env);
        return fd;
    }

}
