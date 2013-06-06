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
package edu.uci.ics.hivesterix.logical.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class HiveExpressionTypeComputer implements IExpressionTypeComputer {

    public static IExpressionTypeComputer INSTANCE = new HiveExpressionTypeComputer();

    @Override
    public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
            IVariableTypeEnvironment env) throws AlgebricksException {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            /**
             * function expression
             */
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            IFunctionInfo funcInfo = funcExpr.getFunctionInfo();

            /**
             * argument expressions, types, object inspectors
             */
            List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();
            List<TypeInfo> argumentTypes = new ArrayList<TypeInfo>();

            /**
             * get types of argument
             */
            for (Mutable<ILogicalExpression> argument : arguments) {
                TypeInfo type = (TypeInfo) getType(argument.getValue(), metadataProvider, env);
                argumentTypes.add(type);
            }

            ObjectInspector[] childrenOIs = new ObjectInspector[argumentTypes.size()];

            /**
             * get object inspector
             */
            for (int i = 0; i < argumentTypes.size(); i++) {
                childrenOIs[i] = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(argumentTypes.get(i));
            }

            /**
             * type inference for scalar function
             */
            if (funcExpr instanceof ScalarFunctionCallExpression) {

                FunctionIdentifier AlgebricksId = funcInfo.getFunctionIdentifier();
                Object functionInfo = ((HiveFunctionInfo) funcInfo).getInfo();
                String udfName = HiveAlgebricksBuiltInFunctionMap.INSTANCE.getHiveFunctionName(AlgebricksId);
                GenericUDF udf;
                if (udfName != null) {
                    /**
                     * get corresponding function info for built-in functions
                     */
                    FunctionInfo fInfo = FunctionRegistry.getFunctionInfo(udfName);
                    udf = fInfo.getGenericUDF();
                } else if (functionInfo != null) {
                    /**
                     * for GenericUDFBridge: we should not call get type of this
                     * hive expression, because parameters may have been
                     * changed!
                     */
                    ExprNodeGenericFuncDesc hiveExpr = (ExprNodeGenericFuncDesc) functionInfo;
                    udf = hiveExpr.getGenericUDF();
                } else {
                    /**
                     * for other generic UDF
                     */
                    Class<?> udfClass;
                    try {
                        udfClass = Class.forName(AlgebricksId.getName());
                        udf = (GenericUDF) udfClass.newInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new AlgebricksException(e.getMessage());
                    }
                }
                /**
                 * doing the actual type inference
                 */
                ObjectInspector oi = null;
                try {
                    oi = udf.initialize(childrenOIs);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                TypeInfo exprType = TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
                return exprType;

            } else if (funcExpr instanceof AggregateFunctionCallExpression) {
                /**
                 * hive aggregation info
                 */
                AggregationDesc aggregateDesc = (AggregationDesc) ((HiveFunctionInfo) funcExpr.getFunctionInfo())
                        .getInfo();
                /**
                 * type inference for aggregation function
                 */
                GenericUDAFEvaluator result = aggregateDesc.getGenericUDAFEvaluator();

                ObjectInspector returnOI = null;
                try {
                    returnOI = result.init(aggregateDesc.getMode(), childrenOIs);
                } catch (HiveException e) {
                    e.printStackTrace();
                }
                TypeInfo exprType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
                return exprType;
            } else if (funcExpr instanceof UnnestingFunctionCallExpression) {
                /**
                 * type inference for UDTF function
                 */
                UDTFDesc hiveDesc = (UDTFDesc) ((HiveFunctionInfo) funcExpr.getFunctionInfo()).getInfo();
                GenericUDTF udtf = hiveDesc.getGenericUDTF();
                ObjectInspector returnOI = null;
                try {
                    returnOI = udtf.initialize(childrenOIs);
                } catch (HiveException e) {
                    e.printStackTrace();
                }
                TypeInfo exprType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
                return exprType;
            } else {
                throw new IllegalStateException("unrecognized function expression " + expr.getClass().getName());
            }
        } else if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            /**
             * get type for variable in the environment
             */
            VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
            LogicalVariable var = varExpr.getVariableReference();
            TypeInfo type = (TypeInfo) env.getVarType(var);
            return type;
        } else if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            /**
             * get type for constant, from its java class
             */
            ConstantExpression constExpr = (ConstantExpression) expr;
            HivesterixConstantValue value = (HivesterixConstantValue) constExpr.getValue();
            TypeInfo type = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(value.getObject().getClass());
            return type;
        } else {
            throw new IllegalStateException("illegal expressions " + expr.getClass().getName());
        }
    }
}
