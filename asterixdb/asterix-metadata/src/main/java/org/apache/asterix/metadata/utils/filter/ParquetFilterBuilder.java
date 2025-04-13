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
import org.apache.logging.log4j.LogManager;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;

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
                LOGGER.error("Error creating Parquet row-group filter expression ", e);
            }
        }
        if (parquetFilterPredicate != null && !(parquetFilterPredicate instanceof Predicate)) {
            parquetFilterPredicate = null;
        }
        return parquetFilterPredicate;
    }

    private FilterPredicate createComparisonExpression(ILogicalExpression columnName, ILogicalExpression constValue,
            FunctionIdentifier fid) throws AlgebricksException {
        ConstantExpression constExpr = (ConstantExpression) constValue;
        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            throw new RuntimeException("Unsupported literal type: " + constExpr.getValue());
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        String fieldName = createColumnExpression(columnName);
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
                throw new RuntimeException("Unsupported literal type: " + constantValue.getObject().getType());
        }
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private FilterPredicate createFilterExpression(ILogicalExpression expr) throws AlgebricksException {
        if (expr == null || expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new RuntimeException("Unsupported expression: " + expr);
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        IFunctionDescriptor fd = resolveFunction(funcExpr);
        FunctionIdentifier fid = fd.getIdentifier();
        if (funcExpr.getArguments().size() != 2
                && !(fid.equals(AlgebricksBuiltinFunctions.AND) || fid.equals(AlgebricksBuiltinFunctions.OR))) {
            throw new RuntimeException("Unsupported function: " + funcExpr);
        }
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (fid.equals(AlgebricksBuiltinFunctions.AND) || fid.equals(AlgebricksBuiltinFunctions.OR)) {
            return createAndOrPredicate(fid, args, 0);
        } else {
            return createComparisonExpression(args.get(0).getValue(), args.get(1).getValue(), fid);
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
            throw new RuntimeException("Unsupported function: " + fid);
        }
    }

    protected String createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Unsupported column expression: " + expression);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            // The field could be a nested field
            List<String> fieldList = new ArrayList<>();
            fieldList = createPathExpression(path, fieldList);
            return String.join(".", fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return path.getFieldNames()[0];
        } else {
            throw new RuntimeException("Unsupported column expression: " + expression);
        }
    }

    private List<String> createPathExpression(ARecordType path, List<String> fieldList) {
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Error creating column expression");
        } else {
            fieldList.add(path.getFieldNames()[0]);
        }
        if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            return createPathExpression((ARecordType) path.getFieldTypes()[0], fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return fieldList;
        } else {
            throw new RuntimeException("Error creating column expression");
        }
    }

    // Converts or(pred1, pred2, pred3) to or(pred1, or(pred2, pred3))
    private FilterPredicate createAndOrPredicate(FunctionIdentifier function, List<Mutable<ILogicalExpression>> args,
            int index) throws AlgebricksException {
        if (index == args.size() - 2) {
            if (function.equals(AlgebricksBuiltinFunctions.AND)) {
                return FilterApi.and(createFilterExpression(args.get(0).getValue()),
                        createFilterExpression(args.get(1).getValue()));
            } else {
                return FilterApi.or(createFilterExpression(args.get(0).getValue()),
                        createFilterExpression(args.get(1).getValue()));
            }
        } else {
            if (function.equals(AlgebricksBuiltinFunctions.AND)) {
                return FilterApi.and(createFilterExpression(args.get(index).getValue()),
                        createAndOrPredicate(function, args, index + 1));
            } else {
                return FilterApi.or(createFilterExpression(args.get(index).getValue()),
                        createAndOrPredicate(function, args, index + 1));
            }
        }
    }
}
