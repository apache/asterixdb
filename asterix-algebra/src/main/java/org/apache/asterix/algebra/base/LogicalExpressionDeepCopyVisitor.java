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
package org.apache.asterix.algebra.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class LogicalExpressionDeepCopyVisitor implements ILogicalExpressionVisitor<ILogicalExpression, Void> {
    private final Counter counter;
    private final Map<LogicalVariable, LogicalVariable> inVarMapping;
    private final Map<LogicalVariable, LogicalVariable> outVarMapping;

    public LogicalExpressionDeepCopyVisitor(Counter counter, Map<LogicalVariable, LogicalVariable> inVarMapping,
            Map<LogicalVariable, LogicalVariable> variableMapping) {
        this.counter = counter;
        this.inVarMapping = inVarMapping;
        this.outVarMapping = variableMapping;
    }

    public ILogicalExpression deepCopy(ILogicalExpression expr) throws AlgebricksException {
        return expr.accept(this, null);
    }

    private void deepCopyAnnotations(AbstractFunctionCallExpression src, AbstractFunctionCallExpression dest) {
        Map<Object, IExpressionAnnotation> srcAnnotations = src.getAnnotations();
        Map<Object, IExpressionAnnotation> destAnnotations = dest.getAnnotations();
        for (Object k : srcAnnotations.keySet()) {
            IExpressionAnnotation annotation = srcAnnotations.get(k).copy();
            destAnnotations.put(k, annotation);
        }
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

    public MutableObject<ILogicalExpression> deepCopyExpressionReference(Mutable<ILogicalExpression> exprRef)
            throws AlgebricksException {
        return new MutableObject<ILogicalExpression>(deepCopy(exprRef.getValue()));
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
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
        return new ConstantExpression(expr.getValue());
    }

    @Override
    public ILogicalExpression visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        ScalarFunctionCallExpression exprCopy = new ScalarFunctionCallExpression(expr.getFunctionInfo(),
                deepCopyExpressionReferenceList(expr.getArguments()));
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        return exprCopy;

    }

    @Override
    public ILogicalExpression visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalExpression visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        UnnestingFunctionCallExpression exprCopy = new UnnestingFunctionCallExpression(expr.getFunctionInfo(),
                deepCopyExpressionReferenceList(expr.getArguments()));
        deepCopyAnnotations(expr, exprCopy);
        deepCopyOpaqueParameters(expr, exprCopy);
        return exprCopy;
    }

    @Override
    public ILogicalExpression visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
            throws AlgebricksException {
        LogicalVariable var = expr.getVariableReference();
        LogicalVariable givenVarReplacement = inVarMapping.get(var);
        if (givenVarReplacement != null) {
            outVarMapping.put(var, givenVarReplacement);
            return new VariableReferenceExpression(givenVarReplacement);
        }
        LogicalVariable varCopy = outVarMapping.get(var);
        if (varCopy == null) {
            counter.inc();
            varCopy = new LogicalVariable(counter.get());
            outVarMapping.put(var, varCopy);
        }
        return new VariableReferenceExpression(varCopy);
    }
}
