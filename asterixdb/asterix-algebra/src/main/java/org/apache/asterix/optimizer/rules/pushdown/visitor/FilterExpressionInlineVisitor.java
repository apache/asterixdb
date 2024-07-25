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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.metadata.utils.PushdownUtil.getArrayConstantFromScanCollection;
import static org.apache.asterix.metadata.utils.PushdownUtil.isSupportedFilterAggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.annotations.ExistsComparisonExpressionAnnotation;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class FilterExpressionInlineVisitor
        implements ILogicalExpressionVisitor<ILogicalExpression, Map<ILogicalOperator, List<UseDescriptor>>> {

    private final PushdownContext pushdownContext;
    private final IOptimizationContext context;
    private final Map<ILogicalOperator, ILogicalExpression> inlinedCache;

    public FilterExpressionInlineVisitor(PushdownContext pushdownContext, IOptimizationContext context) {
        this.pushdownContext = pushdownContext;
        this.context = context;
        inlinedCache = new HashMap<>();
    }

    public ILogicalExpression cloneAndInline(UseDescriptor useDescriptor,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        ILogicalOperator op = useDescriptor.getOperator();
        ILogicalExpression inlinedExpr = inlinedCache.get(op);
        if (inlinedExpr == null) {
            inlinedExpr = useDescriptor.getExpression().accept(this, subplanSelects);
            inlinedCache.put(op, inlinedExpr);
        }

        // Clone the cached expression as a processor may change it
        return inlinedExpr.cloneExpression();
    }

    @Override
    public ILogicalExpression visitConstantExpression(ConstantExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        return expr;
    }

    @Override
    public ILogicalExpression visitVariableReferenceExpression(VariableReferenceExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        LogicalVariable variable = expr.getVariableReference();
        DefineDescriptor defineDescriptor = pushdownContext.getDefineDescriptor(variable);
        if (defineDescriptor == null || defineDescriptor.isScanDefinition()) {
            // Reached un-filterable source variable (e.g., originated from an internal dataset in row format)
            // or filterable source recordVariable (e.g., columnar dataset or external dataset with prefix)
            return expr.cloneExpression();
        }

        ILogicalOperator subplanOp = defineDescriptor.getSubplanOperator();
        ILogicalExpression defExpr = defineDescriptor.getExpression();
        if (subplanOp != null && subplanSelects.containsKey(subplanOp) && isSupportedFilterAggregateFunction(defExpr)) {
            List<UseDescriptor> selects = subplanSelects.get(subplanOp);
            return visitSubplanSelects(selects, subplanSelects);
        }

        return defineDescriptor.getExpression().accept(this, subplanSelects);
    }

    @Override
    public ILogicalExpression visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        return cloneAndInlineFunction(expr, subplanSelects);
    }

    @Override
    public ILogicalExpression visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        ILogicalExpression inlinable = getInlinableExpression(expr);
        if (inlinable.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return inlinable.accept(this, subplanSelects);
        }
        return cloneAndInlineFunction(expr, subplanSelects);
    }

    @Override
    public ILogicalExpression visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        return cloneAndInlineFunction(expr, subplanSelects);
    }

    @Override
    public ILogicalExpression visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        return cloneAndInlineFunction(expr, subplanSelects);
    }

    private ILogicalExpression cloneAndInlineFunction(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        AbstractFunctionCallExpression cloned = (AbstractFunctionCallExpression) funcExpr.cloneExpression();
        for (Mutable<ILogicalExpression> arg : cloned.getArguments()) {
            arg.setValue(arg.getValue().accept(this, subplanSelects));
        }
        return convertToOr(cloned, context);
    }

    /**
     * @param expression current scalar function
     * @return if annotated with {@link ExistsComparisonExpressionAnnotation} then return the count variable expression
     * or return the same function
     */
    private ILogicalExpression getInlinableExpression(ScalarFunctionCallExpression expression) {
        ScalarFunctionCallExpression funcExpr = expression.cloneExpression();
        IExpressionAnnotation existsAnnotation = funcExpr.getAnnotation(ExistsComparisonExpressionAnnotation.class);
        if (existsAnnotation != null) {
            for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
                // Get the variable expression, which is a result from an aggregate
                ILogicalExpression arg = argRef.getValue();
                if (arg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    return arg;
                }
            }
        }
        return funcExpr;
    }

    private boolean notContainsZeroConstant(AbstractFunctionCallExpression funcExpr) {
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            Long argValue = ConstantExpressionUtil.getLongConstant(arg.getValue());
            if (argValue != null && argValue == 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Converts eq(scan-collection(array: [a, b, c...]), expr) to or(eq(a, expr), eq(b, expr), eq(c, expr), ...)
     *
     * @param expression a function expression
     * @return a converted expression if applicable
     */
    private static ILogicalExpression convertToOr(AbstractFunctionCallExpression expression,
            IOptimizationContext context) {
        if (!BuiltinFunctions.EQ.equals(expression.getFunctionIdentifier())) {
            return expression;
        }
        ILogicalExpression left = expression.getArguments().get(0).getValue();
        ILogicalExpression right = expression.getArguments().get(1).getValue();

        ILogicalExpression valueExpr = left;
        AOrderedList constArray = getArrayConstantFromScanCollection(right);
        if (constArray == null) {
            valueExpr = right;
            constArray = getArrayConstantFromScanCollection(left);
        }

        if (constArray == null) {
            return expression;
        }

        IFunctionInfo orInfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.OR);
        List<Mutable<ILogicalExpression>> orArgs = new ArrayList<>();
        AbstractFunctionCallExpression orExpr = new ScalarFunctionCallExpression(orInfo, orArgs);

        IFunctionInfo eqInfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.EQ);
        for (int i = 0; i < constArray.size(); i++) {
            List<Mutable<ILogicalExpression>> eqArgs = new ArrayList<>(2);
            eqArgs.add(new MutableObject<>(valueExpr));
            eqArgs.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(constArray.getItem(i)))));

            orArgs.add(new MutableObject<>(new ScalarFunctionCallExpression(eqInfo, eqArgs)));
        }

        return orExpr;
    }

    private ILogicalExpression visitSubplanSelects(List<UseDescriptor> useDescriptors,
            Map<ILogicalOperator, List<UseDescriptor>> subplanSelects) throws AlgebricksException {
        if (useDescriptors.size() == 1) {
            // A single select exists in the subplan. Inline and clone.
            return useDescriptors.get(0).getExpression().accept(this, subplanSelects);
        }

        // Multiple selects exist in the subplan, inline each then add all inlined expression as a single AND
        List<Mutable<ILogicalExpression>> andArgs = new ArrayList<>();
        for (UseDescriptor useDescriptor : useDescriptors) {
            ILogicalExpression inlined = useDescriptor.getExpression().accept(this, subplanSelects);
            andArgs.add(new MutableObject<>(inlined));
        }

        IFunctionInfo fInfo = context.getMetadataProvider().lookupFunction(BuiltinFunctions.AND);
        return new ScalarFunctionCallExpression(fInfo, andArgs);
    }

    private static FunctionIdentifier[] createExistsPattern() {
        FunctionIdentifier[] pattern = new FunctionIdentifier[2];
        pattern[0] = BuiltinFunctions.NEQ;
        pattern[1] = BuiltinFunctions.COUNT;

        return pattern;
    }
}
