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
package org.apache.asterix.metadata.utils.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.DeltaTableFilterEvaluatorFactory;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.logging.log4j.LogManager;

import com.microsoft.azure.storage.core.Logger;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;

public class DeltaTableFilterBuilder extends AbstractFilterBuilder {

    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger();

    public DeltaTableFilterBuilder(ExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
    }

    public IExternalFilterEvaluatorFactory build() throws AlgebricksException {
        Expression deltaTablePredicate = null;
        if (filterExpression != null) {
            try {
                deltaTablePredicate = createExpression(filterExpression);
            } catch (Exception e) {
                LOGGER.error("Error creating DeltaTable filter expression ", e);
            }
        }
        if (deltaTablePredicate != null && !(deltaTablePredicate instanceof Predicate)) {
            deltaTablePredicate = null;
        }
        return new DeltaTableFilterEvaluatorFactory(deltaTablePredicate);
    }

    protected Expression createExpression(ILogicalExpression expression) throws AlgebricksException {
        if (filterPaths.containsKey(expression)) {
            // Path expression, create a value accessor (i.e., a column reader)
            return createColumnExpression(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return createLiteralExpression(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return handleFunction(expression);
        }

        /*
         * A variable expression: This should not happen as the provided filter expression is inlined.
         * If a variable was encountered for some reason, it should only be the record variable. If the record variable
         * was encountered, that means there's a missing value path the compiler didn't provide.
         */
        throw new RuntimeException("Unsupported expression " + expression + ". the provided paths are: " + filterPaths);
    }

    private Expression createLiteralExpression(ILogicalExpression expression) throws AlgebricksException {
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            throw new RuntimeException("Unsupported literal type: " + constExpr.getValue());
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        switch (constantValue.getObject().getType().getTypeTag()) {
            case STRING:
                return Literal.ofString(((AString) constantValue.getObject()).getStringValue());
            case TINYINT:
                return Literal.ofByte(((AInt8) constantValue.getObject()).getByteValue());
            case SMALLINT:
                return Literal.ofShort(((AInt16) constantValue.getObject()).getShortValue());
            case INTEGER:
                return Literal.ofInt(((AInt32) constantValue.getObject()).getIntegerValue());
            case BOOLEAN:
                return Literal.ofBoolean(constantValue.isTrue());
            case BIGINT:
                return Literal.ofLong(((AInt64) constantValue.getObject()).getLongValue());
            case DOUBLE:
                return Literal.ofDouble(((ADouble) constantValue.getObject()).getDoubleValue());
            case DATE:
                return Literal.ofDate(((ADate) constantValue.getObject()).getChrononTimeInDays());
            case DATETIME:
                Long millis = ((ADateTime) constantValue.getObject()).getChrononTime();
                return Literal.ofTimestamp(TimeUnit.MILLISECONDS.toMicros(millis));
            default:
                throw new RuntimeException("Unsupported literal type: " + constantValue.getObject().getType());
        }
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private Expression handleFunction(ILogicalExpression expr) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        IFunctionDescriptor fd = resolveFunction(funcExpr);
        List<Expression> args = handleArgs(funcExpr);
        FunctionIdentifier fid = fd.getIdentifier();
        if (fid.equals(AlgebricksBuiltinFunctions.AND)) {
            return new Predicate("AND", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.OR)) {
            return new Predicate("OR", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            return new Predicate("=", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return new Predicate(">=", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return new Predicate(">", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return new Predicate("<=", args);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return new Predicate("<", args);
        } else {
            throw new RuntimeException("Unsupported function: " + funcExpr);
        }
    }

    private List<Expression> handleArgs(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        List<Expression> argsExpressions = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression expr = args.get(i).getValue();
            Expression evalFactory = createExpression(expr);
            argsExpressions.add(evalFactory);
        }
        return argsExpressions;
    }

    protected Column createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Unsupported expression: " + expression);
        }
        return new Column(path.getFieldNames()[0]);
    }
}
