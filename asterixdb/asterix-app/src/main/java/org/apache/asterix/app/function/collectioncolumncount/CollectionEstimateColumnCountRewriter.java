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

package org.apache.asterix.app.function.collectioncolumncount;

import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_MISMATCH_FUNCTION;

import java.util.List;

import org.apache.asterix.app.function.FunctionRewriter;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * This function takes a collection's fully qualified name (database.scope.collection) and
 * returns the column count per partition.
 */

public class CollectionEstimateColumnCountRewriter extends FunctionRewriter {

    public static final FunctionIdentifier ESTIMATED_COLLECTION_COLUMN_COUNT =
            FunctionConstants.newAsterix("estimated-collection-column-count", FunctionIdentifier.VARARGS);
    public static final CollectionEstimateColumnCountRewriter INSTANCE =
            new CollectionEstimateColumnCountRewriter(ESTIMATED_COLLECTION_COLUMN_COUNT);

    private CollectionEstimateColumnCountRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
            throws AlgebricksException {

        if (function.getArguments().size() != 3) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS,
                    ESTIMATED_COLLECTION_COLUMN_COUNT.getName());
        }

        verifyArgs(function.getArguments());
        ILogicalExpression databaseExpr = function.getArguments().get(0).getValue();
        ILogicalExpression scopeExpr = function.getArguments().get(1).getValue();
        ILogicalExpression collectionExpr = function.getArguments().get(2).getValue();

        String database = ConstantExpressionUtil.getStringConstant(databaseExpr);
        DataverseName dataverse =
                DataverseName.createSinglePartName(ConstantExpressionUtil.getStringConstant(scopeExpr));
        String collection = ConstantExpressionUtil.getStringConstant(collectionExpr);

        return new CollectionEstimateColumnCountDatasource(context.getComputationNodeDomain(), database, dataverse,
                collection);
    }

    private void verifyArgs(List<Mutable<ILogicalExpression>> args) throws CompilationException {
        for (int i = 0; i < args.size(); i++) {
            ConstantExpression expr = (ConstantExpression) args.get(i).getValue();
            AsterixConstantValue value = (AsterixConstantValue) expr.getValue();
            ATypeTag type = value.getObject().getType().getTypeTag();
            if (type != ATypeTag.STRING) {
                throw new CompilationException(TYPE_MISMATCH_FUNCTION, ESTIMATED_COLLECTION_COLUMN_COUNT.getName(),
                        ExceptionUtil.indexToPosition(i), ATypeTag.STRING, type);
            }
        }
    }
}
