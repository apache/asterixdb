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
package edu.uci.ics.asterix.algebra.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class LogicalExpressionDeepCopyVisitor implements ILogicalExpressionVisitor<ILogicalExpression, Void> {
    private final Counter counter;
    private final Map<LogicalVariable, LogicalVariable> inVarMapping;
    private final Map<LogicalVariable, LogicalVariable> outVarMapping;

    public LogicalExpressionDeepCopyVisitor(Counter counter, Map<LogicalVariable, LogicalVariable> inVarMapping, Map<LogicalVariable, LogicalVariable> variableMapping) {
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

    public MutableObject<ILogicalExpression> deepCopyExpressionReference(Mutable<ILogicalExpression> exprRef)
            throws AlgebricksException {
        return new MutableObject<ILogicalExpression>(deepCopy(exprRef.getValue()));
    }

    // TODO return List<...>
    public ArrayList<Mutable<ILogicalExpression>> deepCopyExpressionReferenceList(List<Mutable<ILogicalExpression>> list)
            throws AlgebricksException {
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
