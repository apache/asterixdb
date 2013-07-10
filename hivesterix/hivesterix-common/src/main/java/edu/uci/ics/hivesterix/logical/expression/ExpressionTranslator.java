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
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class ExpressionTranslator {

    public static Object getHiveExpression(ILogicalExpression expr, IVariableTypeEnvironment env) throws Exception {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            /**
             * function expression
             */
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            IFunctionInfo funcInfo = funcExpr.getFunctionInfo();
            FunctionIdentifier fid = funcInfo.getFunctionIdentifier();

            if (fid.getName().equals(ExpressionConstant.FIELDACCESS)) {
                Object info = ((HiveFunctionInfo) funcInfo).getInfo();
                ExprNodeFieldDesc desc = (ExprNodeFieldDesc) info;
                return new ExprNodeFieldDesc(desc.getTypeInfo(), desc.getDesc(), desc.getFieldName(), desc.getIsList());
            }

            if (fid.getName().equals(ExpressionConstant.NULL)) {
                return new ExprNodeNullDesc();
            }

            /**
             * argument expressions: translate argument expressions recursively
             * first, this logic is shared in scalar, aggregation and unnesting
             * function
             */
            List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();
            List<ExprNodeDesc> parameters = new ArrayList<ExprNodeDesc>();
            for (Mutable<ILogicalExpression> argument : arguments) {
                /**
                 * parameters could not be aggregate function desc
                 */
                ExprNodeDesc parameter = (ExprNodeDesc) getHiveExpression(argument.getValue(), env);
                parameters.add(parameter);
            }

            /**
             * get expression
             */
            if (funcExpr instanceof ScalarFunctionCallExpression) {
                String udfName = HiveAlgebricksBuiltInFunctionMap.INSTANCE.getHiveFunctionName(fid);
                GenericUDF udf;
                if (udfName != null) {
                    /**
                     * get corresponding function info for built-in functions
                     */
                    FunctionInfo fInfo = FunctionRegistry.getFunctionInfo(udfName);
                    udf = fInfo.getGenericUDF();

                    int inputSize = parameters.size();
                    List<ExprNodeDesc> currentDescs = new ArrayList<ExprNodeDesc>();

                    // generate expression tree if necessary
                    while (inputSize > 2) {
                        int pairs = inputSize / 2;
                        for (int i = 0; i < pairs; i++) {
                            List<ExprNodeDesc> descs = new ArrayList<ExprNodeDesc>();
                            descs.add(parameters.get(2 * i));
                            descs.add(parameters.get(2 * i + 1));
                            ExprNodeDesc desc = ExprNodeGenericFuncDesc.newInstance(udf, descs);
                            currentDescs.add(desc);
                        }

                        if (inputSize % 2 != 0) {
                            // List<ExprNodeDesc> descs = new
                            // ArrayList<ExprNodeDesc>();
                            // ExprNodeDesc lastExpr =
                            // currentDescs.remove(currentDescs.size() - 1);
                            // descs.add(lastExpr);
                            currentDescs.add(parameters.get(inputSize - 1));
                            // ExprNodeDesc desc =
                            // ExprNodeGenericFuncDesc.newInstance(udf, descs);
                            // currentDescs.add(desc);
                        }
                        inputSize = currentDescs.size();
                        parameters.clear();
                        parameters.addAll(currentDescs);
                        currentDescs.clear();
                    }

                } else {
                    Object secondInfo = ((HiveFunctionInfo) funcInfo).getInfo();
                    if (secondInfo != null) {

                        /**
                         * for GenericUDFBridge: we should not call get type of
                         * this hive expression, because parameters may have
                         * been changed!
                         */
                        ExprNodeGenericFuncDesc hiveExpr = (ExprNodeGenericFuncDesc) ((HiveFunctionInfo) funcInfo)
                                .getInfo();
                        udf = hiveExpr.getGenericUDF();
                    } else {
                        /**
                         * for other generic UDF
                         */
                        Class<?> udfClass;
                        try {
                            udfClass = Class.forName(fid.getName());
                            udf = (GenericUDF) udfClass.newInstance();
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new AlgebricksException(e.getMessage());
                        }
                    }
                }
                /**
                 * get hive generic function expression
                 */
                ExprNodeDesc desc = ExprNodeGenericFuncDesc.newInstance(udf, parameters);
                return desc;
            } else if (funcExpr instanceof AggregateFunctionCallExpression) {
                /**
                 * hive aggregation info
                 */
                AggregationDesc aggregateDesc = (AggregationDesc) ((HiveFunctionInfo) funcExpr.getFunctionInfo())
                        .getInfo();
                /**
                 * set parameters
                 */
                aggregateDesc.setParameters((ArrayList<ExprNodeDesc>) parameters);

                List<TypeInfo> originalParameterTypeInfos = new ArrayList<TypeInfo>();
                for (ExprNodeDesc parameter : parameters) {
                    if (parameter.getTypeInfo() instanceof StructTypeInfo) {
                        originalParameterTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
                    } else
                        originalParameterTypeInfos.add(parameter.getTypeInfo());
                }

                List<ObjectInspector> originalParameterOIs = new ArrayList<ObjectInspector>();
                for (TypeInfo type : originalParameterTypeInfos) {
                    originalParameterOIs.add(LazyUtils.getLazyObjectInspectorFromTypeInfo(type, false));
                }
                GenericUDAFEvaluator eval = FunctionRegistry.getGenericUDAFEvaluator(
                        aggregateDesc.getGenericUDAFName(), originalParameterOIs, aggregateDesc.getDistinct(), false);

                AggregationDesc newAggregateDesc = new AggregationDesc(aggregateDesc.getGenericUDAFName(), eval,
                        aggregateDesc.getParameters(), aggregateDesc.getDistinct(), aggregateDesc.getMode());
                return newAggregateDesc;
            } else if (funcExpr instanceof UnnestingFunctionCallExpression) {
                /**
                 * type inference for UDTF function
                 */
                UDTFDesc hiveDesc = (UDTFDesc) ((HiveFunctionInfo) funcExpr.getFunctionInfo()).getInfo();
                String funcName = hiveDesc.getUDTFName();
                FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
                GenericUDTF udtf = fi.getGenericUDTF();
                UDTFDesc desc = new UDTFDesc(udtf);
                return desc;
            } else {
                throw new IllegalStateException("unrecognized function expression " + expr.getClass().getName());
            }
        } else if ((expr.getExpressionTag() == LogicalExpressionTag.VARIABLE)) {
            /**
             * get type for variable in the environment
             */
            VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
            LogicalVariable var = varExpr.getVariableReference();
            TypeInfo typeInfo = (TypeInfo) env.getVarType(var);
            ExprNodeDesc desc = new ExprNodeColumnDesc(typeInfo, var.toString(), "", false);
            return desc;
        } else if ((expr.getExpressionTag() == LogicalExpressionTag.CONSTANT)) {
            /**
             * get expression for constant in the environment
             */
            ConstantExpression varExpr = (ConstantExpression) expr;
            Object value = ((HivesterixConstantValue) varExpr.getValue()).getObject();
            ExprNodeDesc desc = new ExprNodeConstantDesc(value);
            return desc;
        } else {
            throw new IllegalStateException("illegal expressions " + expr.getClass().getName());
        }
    }
}
