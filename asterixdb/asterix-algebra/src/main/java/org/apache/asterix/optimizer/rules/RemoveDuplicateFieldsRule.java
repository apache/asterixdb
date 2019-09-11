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
package org.apache.asterix.optimizer.rules;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.ClosedRecordConstructorResultType;
import org.apache.asterix.om.typecomputer.impl.OpenRecordConstructorResultType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * <pre>
 * This rule removes duplicate fields from record constructors. For example:
 * {"f1": 87, "f2": "val2", "f1": "str"}.
 * For records where the field name is not a constant string, {@link ConstantFoldingRule} will evaluate the
 * expression and remove the field if it is a duplicate. If the field name expression cannot be constant folded, then
 * {@link RecordBuilder} will take care of not adding the field at runtime if it is a duplicate. Such field names
 * will be added in the open part of the record. Examples:
 * <ol>
 *     <li>{"f1": 11, lowercase("F1"): 12}</li>
 *     <li>{"f1": 11, lowercase($$var): 12}</li>
 * </ol>
 *
 * Note: {@link OpenRecordConstructorResultType} and {@link ClosedRecordConstructorResultType} still have a sanity
 * check that tests that there are no duplicate fields during compilation.
 * </pre>
 */
public class RemoveDuplicateFieldsRule implements IAlgebraicRewriteRule {

    private final Set<String> fieldNames = new HashSet<>();
    private IOptimizationContext context;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        if (ctx.checkIfInDontApplySet(this, opRef.getValue())) {
            // children should've already been visited and marked through the root operator
            return false;
        }
        context = ctx;
        return rewriteOpAndInputs(opRef, true);
    }

    private boolean rewriteOpAndInputs(Mutable<ILogicalOperator> opRef, boolean first) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (!first) {
            context.addToDontApplySet(this, op);
        }
        List<Mutable<ILogicalOperator>> inputs = op.getInputs();
        boolean changed = false;
        for (int i = 0, size = inputs.size(); i < size; i++) {
            changed |= rewriteOpAndInputs(inputs.get(i), false);
        }
        changed |= op.acceptExpressionTransform(this::transform);
        return changed;
    }

    private boolean transform(Mutable<ILogicalExpression> expressionRef) throws AlgebricksException {
        ILogicalExpression expr = expressionRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression function = (AbstractFunctionCallExpression) expr;
        boolean changed = false;
        if (function.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                || function.getFunctionIdentifier().equals(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            if (function.getArguments().size() % 2 != 0) {
                String functionName = function.getFunctionIdentifier().getName();
                throw CompilationException.create(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                        functionName);
            }
            fieldNames.clear();
            Iterator<Mutable<ILogicalExpression>> iterator = function.getArguments().iterator();
            while (iterator.hasNext()) {
                ILogicalExpression fieldNameExpr = iterator.next().getValue();
                String fieldName = ConstantExpressionUtil.getStringConstant(fieldNameExpr);
                if (fieldName != null && !fieldNames.add(fieldName)) {
                    IWarningCollector warningCollector = context.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(WarningUtil.forAsterix(fieldNameExpr.getSourceLocation(),
                                ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, fieldName));
                    }
                    iterator.remove();
                    iterator.next();
                    iterator.remove();
                    changed = true;
                } else {
                    iterator.next();
                }
            }
        }
        List<Mutable<ILogicalExpression>> arguments = function.getArguments();
        for (int i = 0, size = arguments.size(); i < size; i++) {
            changed |= transform(arguments.get(i));
        }
        return changed;
    }
}
