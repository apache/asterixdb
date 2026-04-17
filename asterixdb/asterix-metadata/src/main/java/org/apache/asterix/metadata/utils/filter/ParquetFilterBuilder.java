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
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

public class ParquetFilterBuilder extends AbstractFilterBuilder {

    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger();

    public ParquetFilterBuilder(ParquetExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getParquetRowGroupFilterExpression(),
                context, typeEnv);
    }

    public FilterPredicate buildFilterPredicate() throws AlgebricksException {
        FilterPredicate parquetFilterPredicate = null;
        if (filterExpression != null) {
            try {
                parquetFilterPredicate = createFilterExpression(filterExpression);
            } catch (Exception e) {
                LOGGER.error("Error creating Parquet row-group filter expression ", e.getMessage());
            }
        }
        return parquetFilterPredicate;
    }

    private FilterPredicate createComparisonExpression(ILogicalExpression arg1, ILogicalExpression arg2,
            FunctionIdentifier fid) throws AlgebricksException {
        ILogicalExpression columnName;
        ConstantExpression constExpr;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constExpr = (ConstantExpression) arg1;
            columnName = arg2;
        } else if (arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constExpr = (ConstantExpression) arg2;
            columnName = arg1;
        } else {
            return null;
        }

        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            return null;
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        String fieldName = createColumnExpression(columnName);
        if (fieldName == null) {
            return null;
        }
        switch (constantValue.getObject().getType().getTypeTag()) {
            case STRING:
                return createComparisionFunction(FilterApi.binaryColumn(fieldName),
                        Binary.fromString(((AString) constantValue.getObject()).getStringValue()), fid);
            case TINYINT:
                return createComparisionFunction(FilterApi.intColumn(fieldName),
                        (int) ((AInt8) constantValue.getObject()).getByteValue(), fid);
            case SMALLINT:
                return createComparisionFunction(FilterApi.intColumn(fieldName),
                        (int) ((AInt16) constantValue.getObject()).getShortValue(), fid);
            case INTEGER:
                return createComparisionFunction(FilterApi.intColumn(fieldName),
                        ((AInt32) constantValue.getObject()).getIntegerValue(), fid);
            case BOOLEAN:
                if (!fid.equals(AlgebricksBuiltinFunctions.EQ)) {
                    throw new RuntimeException("Unsupported comparison function: " + fid);
                }
                return FilterApi.eq(FilterApi.booleanColumn(fieldName), constantValue.isTrue());
            case BIGINT:
                return createComparisionFunction(FilterApi.longColumn(fieldName),
                        ((AInt64) constantValue.getObject()).getLongValue(), fid);
            case DOUBLE:
                return createComparisionFunction(FilterApi.doubleColumn(fieldName),
                        ((ADouble) constantValue.getObject()).getDoubleValue(), fid);
            case DATE:
                return createComparisionFunction(FilterApi.intColumn(fieldName),
                        ((ADate) constantValue.getObject()).getChrononTimeInDays(), fid);
            case DATETIME:
                Long millis = ((ADateTime) constantValue.getObject()).getChrononTime();
                return createComparisionFunction(FilterApi.longColumn(fieldName),
                        TimeUnit.MILLISECONDS.toMicros(millis), fid);
            default:
                return null;
        }
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private FilterPredicate createFilterExpression(ILogicalExpression expr) throws AlgebricksException {
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
            FilterPredicate filterPredicate = createAndOrPredicate(fid, args, 0, args.size());
            if (filterPredicate == null) {
                LOGGER.info("Unable to construct row group filter with OR/AND expression");
            }
            return filterPredicate;
        } else {
            FilterPredicate filterPredicate =
                    createComparisonExpression(args.get(0).getValue(), args.get(1).getValue(), fid);
            if (filterPredicate == null) {
                LOGGER.info("Unable to construct row group filter");
            }
            return filterPredicate;
        }
    }

    private <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate createComparisionFunction(
            C column, T value, FunctionIdentifier fid) {
        if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            return FilterApi.eq(column, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return FilterApi.gtEq(column, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return FilterApi.gt(column, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return FilterApi.ltEq(column, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return FilterApi.lt(column, value);
        } else {
            return null;
        }
    }

    protected String createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            return null;
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            // The field could be a nested field
            List<String> fieldList = new ArrayList<>();
            fieldList = createPathExpression(path, fieldList);
            return String.join(".", fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return path.getFieldNames()[0];
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
    private FilterPredicate createAndOrPredicate(FunctionIdentifier function, List<Mutable<ILogicalExpression>> args,
            int leftInclusive, int rightExclusive) throws AlgebricksException {
        if (rightExclusive - leftInclusive == 1) {
            return createLeafFilterPredicate(args.get(leftInclusive));
        } else {
            FilterPredicate left, right;
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
                    return FilterApi.and(left, right);
                }

            } else {
                if (left == null || right == null) {
                    return null;
                }
                return FilterApi.or(left, right);
            }
        }
    }

    private FilterPredicate createLeafFilterPredicate(Mutable<ILogicalExpression> expression)
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
