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
package org.apache.asterix.app.function;

import static org.apache.asterix.common.exceptions.ErrorCode.EXPECTED_CONSTANT_VALUE;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.exceptions.UnsupportedTypeException;
import org.apache.asterix.om.functions.IFunctionToDataSourceRewriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class FunctionRewriter implements IFunctionToDataSourceRewriter {

    FunctionIdentifier functionId;

    public FunctionRewriter(FunctionIdentifier functionId) {
        this.functionId = functionId;
    }

    @Override
    public final boolean rewrite(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractFunctionCallExpression f = UnnestToDataScanRule.getFunctionCall(opRef);
        List<Mutable<ILogicalExpression>> args = f.getArguments();
        if (args.size() != functionId.getArity()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, f.getSourceLocation(),
                    "Function " + functionId.getNamespace() + "." + functionId.getName() + " expects "
                            + functionId.getArity() + " arguments");
        }
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression argExpr = args.get(i).getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, argExpr.getSourceLocation(),
                        "Function " + functionId.getNamespace() + "." + functionId.getName()
                                + " expects constant arguments while arg[" + i + "] is of type "
                                + argExpr.getExpressionTag());
            }
        }
        UnnestOperator unnest = (UnnestOperator) opRef.getValue();
        if (unnest.getPositionalVariable() != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, unnest.getSourceLocation(),
                    "No positional variables are allowed over datasource functions");
        }
        FunctionDataSource datasource = toDatasource(context, f);
        List<LogicalVariable> variables = new ArrayList<>();
        variables.add(unnest.getVariable());
        DataSourceScanOperator scan = new DataSourceScanOperator(variables, datasource);
        scan.setSourceLocation(unnest.getSourceLocation());
        List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
        scanInpList.addAll(unnest.getInputs());
        opRef.setValue(scan);
        context.computeAndSetTypeEnvironmentForOperator(scan);
        return true;
    }

    protected abstract FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression f)
            throws AlgebricksException;

    protected String getString(SourceLocation loc, List<Mutable<ILogicalExpression>> args, int i)
            throws AlgebricksException {
        IAObject iaObject = ConstantExpressionUtil.getConstantIaObject(args.get(i).getValue(), null);
        if (iaObject == null) {
            throw new CompilationException(EXPECTED_CONSTANT_VALUE, loc);
        }
        ATypeTag tt = iaObject.getType().getTypeTag();
        if (tt != ATypeTag.STRING) {
            throw new UnsupportedTypeException(loc, functionId, tt);
        }
        return ((AString) iaObject).getStringValue();
    }

    protected DataverseName getDataverseName(SourceLocation loc, List<Mutable<ILogicalExpression>> args, int i)
            throws AlgebricksException {
        IAObject iaObject = ConstantExpressionUtil.getConstantIaObject(args.get(i).getValue(), null);
        if (iaObject == null) {
            throw new CompilationException(EXPECTED_CONSTANT_VALUE, loc);
        }
        ATypeTag tt = iaObject.getType().getTypeTag();
        switch (tt) {
            case STRING:
                AString str = (AString) iaObject;
                return DataverseName.createSinglePartName(str.getStringValue());
            case ARRAY:
                AOrderedList list = ((AOrderedList) iaObject);
                int ln = list.size();
                List<String> parts = new ArrayList<>(ln);
                for (int j = 0; j < ln; j++) {
                    IAObject item = list.getItem(j);
                    ATypeTag itt = item.getType().getTypeTag();
                    if (itt != ATypeTag.STRING) {
                        throw new UnsupportedTypeException(loc, functionId, itt);
                    }
                    parts.add(((AString) item).getStringValue());
                }
                return DataverseName.create(parts);
            default:
                throw new UnsupportedTypeException(loc, functionId, tt);
        }
    }
}
