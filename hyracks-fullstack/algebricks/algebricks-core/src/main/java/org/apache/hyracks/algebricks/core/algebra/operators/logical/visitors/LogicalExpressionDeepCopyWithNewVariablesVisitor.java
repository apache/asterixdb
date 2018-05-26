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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IVariableContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class LogicalExpressionDeepCopyWithNewVariablesVisitor
        implements ILogicalExpressionVisitor<ILogicalExpression, Void> {
    private final IVariableContext varContext;
    private final Map<LogicalVariable, LogicalVariable> inVarMapping;
    private final Map<LogicalVariable, LogicalVariable> outVarMapping;
    private final Set<LogicalVariable> freeVars;

    public LogicalExpressionDeepCopyWithNewVariablesVisitor(IVariableContext varContext,
            Map<LogicalVariable, LogicalVariable> inVarMapping, Map<LogicalVariable, LogicalVariable> variableMapping,
            Set<LogicalVariable> freeVars) {
        this.varContext = varContext;
        this.inVarMapping = inVarMapping;
        this.outVarMapping = variableMapping;
        this.freeVars = freeVars;
    }

    public ILogicalExpression deepCopy(ILogicalExpression expr) throws AlgebricksException {
        if (expr == null) {
            return null;
        }
        return expr.accept(this, null);
    }

    private void deepCopyAnnotations(AbstractFunctionCallExpression src, AbstractFunctionCallExpression dest) {
        Map<Object, IExpressionAnnotation> srcAnnotations = src.getAnnotations();
        Map<Object, IExpressionAnnotation> destAnnotations = dest.getAnnotations();
        srcAnnotations.forEach((key, value) -> destAnnotations.put(key, value.copy()));
    }

    private void deepCopyOpaqueParameters(AbstractFunctionCallExpression src, AbstractFunctionCallExpression dest) {
        Object[] srcOpaqueParameters = src.getOpaqueParameters();
        Object[] newOpaqueParameters = null;
        if (srcOpaqueParameters != null) {
            newOpaqueParameters = new Object[srcOpaqueParameters.length];
            for (int i = 0; i < srcOpaqueParameters.length; i++) {
                newOpaqueParameters[i] = srcOpaqueParameters[i];
            }
        }
        dest.setOpaqueParameters(newOpaqueParameters);
    }

    private void copySourceLocation(ILogicalExpression src, AbstractLogicalExpression dest) {
        dest.setSourceLocation(src.getSourceLocation());
    }

    public MutableObject<ILogicalExpression> deepCopyExpressionReference(Mutable<ILogicalExpression> exprRef)
            throws AlgebricksException {
        return new MutableObject<>(deepCopy(exprRef.getValue()));
    }

    // TODO return List<...>
    public ArrayList<Mutable<ILogicalExpression>> deepCopyExpressionReferenceList(
            List<Mutable<ILogicalExpression>> list) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> listCopy = new ArrayList<Mutable<ILogicalExpression>>(list.size());
        for (Mutable<ILogicalExpression> exprRef : list) {
            listCopy.add(deepCopyExpressionReference(exprRef));
        }
        return listCopy;
    }

    @Override
    public ILogicalExpression visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        AggregateFunctionCallExpression exprCopy = new AggregateFunctionCallExpression(expr.getFunctionInfo(),
                expr.isTwoStep(), deepCopyExpressionReferenceList(expr.getArguments()));
        exprCopy.setStepOneAggregate(expr.getStepOneAggregate());
        exprCopy.setStepTwoAggregate(expr.getStepTwoAggregate());
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        copySourceLocation(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
        ConstantExpression exprCopy = new ConstantExpression(expr.getValue());
        copySourceLocation(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        ScalarFunctionCallExpression exprCopy = new ScalarFunctionCallExpression(expr.getFunctionInfo(),
                deepCopyExpressionReferenceList(expr.getArguments()));
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        copySourceLocation(expr, exprCopy);
        return exprCopy;

    }

    @Override
    public ILogicalExpression visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        StatefulFunctionCallExpression exprCopy = new StatefulFunctionCallExpression(expr.getFunctionInfo(),
                expr.getPropertiesComputer(), deepCopyExpressionReferenceList(expr.getArguments()));
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        copySourceLocation(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        UnnestingFunctionCallExpression exprCopy = new UnnestingFunctionCallExpression(expr.getFunctionInfo(),
                deepCopyExpressionReferenceList(expr.getArguments()));
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        copySourceLocation(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
            throws AlgebricksException {
        LogicalVariable var = expr.getVariableReference();
        if (freeVars.contains(var)) {
            return expr;
        }
        LogicalVariable givenVarReplacement = inVarMapping.get(var);
        if (givenVarReplacement != null) {
            outVarMapping.put(var, givenVarReplacement);
            VariableReferenceExpression varRef = new VariableReferenceExpression(givenVarReplacement);
            copySourceLocation(expr, varRef);
            return varRef;
        }
        LogicalVariable varCopy = outVarMapping.get(var);
        if (varCopy == null) {
            varCopy = varContext.newVar();
            outVarMapping.put(var, varCopy);
        }
        VariableReferenceExpression varRef = new VariableReferenceExpression(varCopy);
        copySourceLocation(expr, varRef);
        return varRef;
    }
}
