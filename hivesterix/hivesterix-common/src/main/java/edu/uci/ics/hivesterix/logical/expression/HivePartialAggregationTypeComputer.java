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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class HivePartialAggregationTypeComputer implements IPartialAggregationTypeComputer {

    public static IPartialAggregationTypeComputer INSTANCE = new HivePartialAggregationTypeComputer();

    @Override
    public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            IExpressionTypeComputer tc = HiveExpressionTypeComputer.INSTANCE;
            /**
             * function expression
             */
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            /**
             * argument expressions, types, object inspectors
             */
            List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();
            List<TypeInfo> argumentTypes = new ArrayList<TypeInfo>();

            /**
             * get types of argument
             */
            for (Mutable<ILogicalExpression> argument : arguments) {
                TypeInfo type = (TypeInfo) tc.getType(argument.getValue(), metadataProvider, env);
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
            if (funcExpr instanceof AggregateFunctionCallExpression) {
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
                    returnOI = result.init(getPartialMode(aggregateDesc.getMode()), childrenOIs);
                } catch (HiveException e) {
                    e.printStackTrace();
                }
                TypeInfo exprType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
                return exprType;
            } else {
                throw new IllegalStateException("illegal expressions " + expr.getClass().getName());
            }
        } else {
            throw new IllegalStateException("illegal expressions " + expr.getClass().getName());
        }
    }

    private Mode getPartialMode(Mode mode) {
        Mode partialMode;
        if (mode == Mode.FINAL)
            partialMode = Mode.PARTIAL2;
        else if (mode == Mode.COMPLETE)
            partialMode = Mode.PARTIAL1;
        else
            partialMode = mode;
        return partialMode;
    }
}
