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

import org.apache.asterix.external.input.filter.ParquetFilterExpression;
import org.apache.asterix.external.input.filter.ParquetFilterExpression.Comparison;
import org.apache.asterix.external.input.filter.ParquetFilterExpression.Operator;
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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.projection.ParquetExternalDatasetProjectionFiltrationInfo;
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
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;

/**
 * Builds the row-group filter pushed into Parquet readers.
 * <p>
 * The result names columns and literals without choosing a physical Parquet type for either; that choice belongs
 * to the node reading a particular file, and is made by
 * {@code org.apache.asterix.external.input.filter.ParquetFilterConverter}.
 */
public class ParquetFilterBuilder extends AbstractFilterBuilder {

    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger();

    public ParquetFilterBuilder(ParquetExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getParquetRowGroupFilterExpression(),
                context, typeEnv);
    }

    public ParquetFilterExpression buildFilterExpression() throws AlgebricksException {
        ParquetFilterExpression expression = null;
        if (filterExpression != null) {
            try {
                expression = createFilterExpression(filterExpression);
            } catch (Exception e) {
                LOGGER.error("Error creating Parquet row-group filter expression ", e.getMessage());
            }
        }
        return expression;
    }

    private ParquetFilterExpression createComparisonExpression(ILogicalExpression arg1, ILogicalExpression arg2,
            FunctionIdentifier fid) throws AlgebricksException {
        ILogicalExpression columnName;
        ConstantExpression constExpr;
        boolean constantOnLeft;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constExpr = (ConstantExpression) arg1;
            columnName = arg2;
            constantOnLeft = true;
        } else if (arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constExpr = (ConstantExpression) arg2;
            columnName = arg1;
            constantOnLeft = false;
        } else {
            return null;
        }

        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            return null;
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        String[] path = createColumnExpression(columnName);
        if (path == null) {
            return null;
        }
        Operator operator = toOperator(fid);
        if (operator == null) {
            return null;
        }
        // a comparison reads column-then-literal, so a literal on the left reverses it: 5 < x is x > 5
        if (constantOnLeft) {
            operator = flip(operator);
        }

        ATypeTag tag = constantValue.getObject().getType().getTypeTag();
        Object value;
        switch (tag) {
            case STRING:
                value = ((AString) constantValue.getObject()).getStringValue();
                break;
            case TINYINT:
                value = (long) ((AInt8) constantValue.getObject()).getByteValue();
                break;
            case SMALLINT:
                value = (long) ((AInt16) constantValue.getObject()).getShortValue();
                break;
            case INTEGER:
                value = (long) ((AInt32) constantValue.getObject()).getIntegerValue();
                break;
            case BIGINT:
                value = ((AInt64) constantValue.getObject()).getLongValue();
                break;
            case BOOLEAN:
                if (operator != Operator.EQ) {
                    return null;
                }
                value = constantValue.isTrue();
                break;
            case DOUBLE:
                value = ((ADouble) constantValue.getObject()).getDoubleValue();
                break;
            case DATE:
                value = (long) ((ADate) constantValue.getObject()).getChrononTimeInDays();
                break;
            case DATETIME:
                // milliseconds; the node converts to whatever unit the file stores
                value = ((ADateTime) constantValue.getObject()).getChrononTime();
                break;
            default:
                return null;
        }
        return new Comparison(path, operator, tag, value);
    }

    private static Operator toOperator(FunctionIdentifier fid) {
        if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            return Operator.EQ;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return Operator.GT_EQ;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return Operator.GT;
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return Operator.LT_EQ;
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return Operator.LT;
        } else {
            return null;
        }
    }

    private static Operator flip(Operator operator) {
        switch (operator) {
            case GT:
                return Operator.LT;
            case GT_EQ:
                return Operator.LT_EQ;
            case LT:
                return Operator.GT;
            case LT_EQ:
                return Operator.GT_EQ;
            default:
                return operator;
        }
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private ParquetFilterExpression createFilterExpression(ILogicalExpression expr) throws AlgebricksException {
        if (expr == null || expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            LOGGER.info("Unsupported expression for row group filter: "
                    + LogRedactionUtil.userData(expr == null ? "NULL" : expr.toString()));
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        IFunctionDescriptor fd = resolveFunction(funcExpr);
        FunctionIdentifier fid = fd.getIdentifier();
        if (funcExpr.getArguments().size() != 2
                && !(fid.equals(AlgebricksBuiltinFunctions.AND) || fid.equals(AlgebricksBuiltinFunctions.OR))) {
            LOGGER.info("Unsupported function for row group filter: Unsupported function: "
                    + LogRedactionUtil.userData(expr.toString()));
            return null;
        }
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (fid.equals(AlgebricksBuiltinFunctions.AND) || fid.equals(AlgebricksBuiltinFunctions.OR)) {
            ParquetFilterExpression expression = createAndOrPredicate(fid, args, 0, args.size());
            if (expression == null) {
                LOGGER.info("Unable to construct row group filter with OR/AND expression");
            }
            return expression;
        } else {
            ParquetFilterExpression expression =
                    createComparisonExpression(args.get(0).getValue(), args.get(1).getValue(), fid);
            if (expression == null) {
                LOGGER.info("Unable to construct row group filter");
            }
            return expression;
        }
    }

    protected String[] createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            return null;
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            // The field could be a nested field
            List<String> fieldList = new ArrayList<>();
            fieldList = createPathExpression(path, fieldList);
            return fieldList == null ? null : fieldList.toArray(new String[0]);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return new String[] { path.getFieldNames()[0] };
        } else {
            return null;
        }
    }

    private List<String> createPathExpression(ARecordType path, List<String> fieldList) {
        if (path.getFieldNames().length != 1) {
            return null;
        } else {
            fieldList.add(path.getFieldNames()[0]);
        }
        if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            return createPathExpression((ARecordType) path.getFieldTypes()[0], fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return fieldList;
        } else {
            return null;
        }
    }

    // Converts or(pred1, pred2, pred3) to or(pred1, or(pred2, pred3))
    private ParquetFilterExpression createAndOrPredicate(FunctionIdentifier function,
            List<Mutable<ILogicalExpression>> args, int leftInclusive, int rightExclusive) throws AlgebricksException {
        if (rightExclusive - leftInclusive == 1) {
            return createLeafFilterPredicate(args.get(leftInclusive));
        } else {
            ParquetFilterExpression left, right;
            if (rightExclusive - leftInclusive == 2) {
                left = createLeafFilterPredicate(args.get(leftInclusive));
                right = createLeafFilterPredicate(args.get(leftInclusive + 1));
            } else {
                int middle = (leftInclusive + rightExclusive) / 2;
                left = createAndOrPredicate(function, args, leftInclusive, middle);
                right = createAndOrPredicate(function, args, middle, rightExclusive);
            }

            if (function.equals(AlgebricksBuiltinFunctions.AND)) {
                if (left == null && right == null) {
                    return null;
                } else if (left == null) {
                    return right;
                } else if (right == null) {
                    return left;
                } else {
                    return new ParquetFilterExpression.And(left, right);
                }

            } else {
                if (left == null || right == null) {
                    return null;
                }
                return new ParquetFilterExpression.Or(left, right);
            }
        }
    }

    private ParquetFilterExpression createLeafFilterPredicate(Mutable<ILogicalExpression> expression)
            throws AlgebricksException {
        if (expression.get().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expression.get();
            if (functionCall.getArguments().size() != 2) {
                return null;
            }
            return createComparisonExpression(functionCall.getArguments().get(0).get(),
                    functionCall.getArguments().get(1).get(), functionCall.getFunctionIdentifier());
        } else {
            return null;
        }
    }
}
