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

package org.apache.asterix.optimizer.rules.cbo;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared helpers to construct physical plan fragments of rangeMap used by
 * both optimizer rules and CBO code paths.
 */
public final class RangeMapUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private static FunctionIdentifier rangeMapFunction = BuiltinFunctions.RANGE_MAP;
    private static FunctionIdentifier localSamplingFun = BuiltinFunctions.LOCAL_SAMPLING;
    private static FunctionIdentifier typePropagatingFun = BuiltinFunctions.NULL_WRITER;

    /**
     * Creates the sampling expressions and embeds them in {@code localAggFunctions} & {@code globalAggFunctions}. Also,
     * creates the variables which will hold the result of each one.
     * {@code localResultVariables},{@code localAggFunctions},{@code globalResultVariables} & {@code globalAggFunctions}
     * will be used when creating the corresponding aggregate operators.
     * @param context used to get new variables which will be assigned the samples & the range map
     * @param localResultVariables the variable to which the stats (e.g. samples) info is assigned
     * @param localAggFunctions the local sampling expression and columns expressions are added to this list
     * @param globalResultVariable the variable to which the range map is assigned
     * @param globalAggFunction the expression generating a range map is added to this list
     * @param numPartitions passed to the expression generating a range map to know how many split points are needed
     * @param partitionFields the fields based on which the partitioner partitions the tuples, also sampled fields
     * @param sourceLocation source location
     */
    private static void createAggregateFunction(IOptimizationContext context,
            List<LogicalVariable> localResultVariables, List<Mutable<ILogicalExpression>> localAggFunctions,
            List<LogicalVariable> globalResultVariable, List<Mutable<ILogicalExpression>> globalAggFunction,
            int numPartitions, List<OrderColumn> partitionFields, SourceLocation sourceLocation) {
        // prepare the arguments to the local sampling function: sampled fields (e.g. $col1, $col2)
        // local info: local agg [$1, $2, $3] = [local-sampling-fun($col1, $col2), type_expr($col1), type_expr($col2)]
        // global info: global agg [$RM] = [global-range-map($1, $2, $3)]
        IFunctionInfo samplingFun = context.getMetadataProvider().lookupFunction(localSamplingFun);
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(partitionFields.size());
        List<Mutable<ILogicalExpression>> argsToRM = new ArrayList<>(1 + partitionFields.size());
        AbstractFunctionCallExpression expr = new AggregateFunctionCallExpression(samplingFun, false, fields);
        expr.setSourceLocation(sourceLocation);
        expr.setOpaqueParameters(new Object[] { context.getPhysicalOptimizationConfig().getSortSamples() });
        // add the sampling function to the list of the local functions
        LogicalVariable localOutVariable = context.newVar();
        localResultVariables.add(localOutVariable);
        localAggFunctions.add(new MutableObject<>(expr));
        // add the local result variable as input to the global range map function
        AbstractLogicalExpression varExprRef = new VariableReferenceExpression(localOutVariable, sourceLocation);
        argsToRM.add(new MutableObject<>(varExprRef));
        int i = 0;
        boolean[] ascendingFlags = new boolean[partitionFields.size()];
        IFunctionInfo typeFun = context.getMetadataProvider().lookupFunction(typePropagatingFun);
        for (OrderColumn field : partitionFields) {
            // add the field to the "fields" which is the input to the local sampling function
            varExprRef = new VariableReferenceExpression(field.getColumn(), sourceLocation);
            fields.add(new MutableObject<>(varExprRef));
            // add the same field as input to the corresponding local function propagating the type of the field
            expr = new AggregateFunctionCallExpression(typeFun, false,
                    Collections.singletonList(new MutableObject<>(varExprRef.cloneExpression())));
            // add the type propagating function to the list of the local functions
            localOutVariable = context.newVar();
            localResultVariables.add(localOutVariable);
            localAggFunctions.add(new MutableObject<>(expr));
            // add the local result variable as input to the global range map function
            varExprRef = new VariableReferenceExpression(localOutVariable, sourceLocation);
            argsToRM.add(new MutableObject<>(varExprRef));
            ascendingFlags[i] = field.getOrder() == OrderOperator.IOrder.OrderKind.ASC;
            i++;
        }
        IFunctionInfo rangeMapFun = context.getMetadataProvider().lookupFunction(rangeMapFunction);

        // 'isTwoStep' is set to 'false' to avoid interference with the local and global aggregation stages we've manually defined.
        // If 'isTwoStep' were true, 'IntroduceAggregateCombinerRule' would attempt to apply additional
        // aggregation steps, which would conflict with our already defined aggregation pipeline.
        // This could lead to runtime errors. By keeping 'isTwoStep=false', we ensure no redundant aggregation stages are introduced.
        AggregateFunctionCallExpression rangeMapExp = new AggregateFunctionCallExpression(rangeMapFun, false, argsToRM);
        rangeMapExp.setStepOneAggregate(samplingFun);
        rangeMapExp.setStepTwoAggregate(rangeMapFun);
        rangeMapExp.setSourceLocation(sourceLocation);
        rangeMapExp.setOpaqueParameters(new Object[] { numPartitions, ascendingFlags });
        globalResultVariable.add(context.newVar());
        globalAggFunction.add(new MutableObject<>(rangeMapExp));
    }

    /**
     * Creates an aggregate operator. $$resultVariables = expressions()
     * @param resultVariables the variables which stores the result of the aggregation
     * @param isGlobal whether the aggregate operator is a global or local one
     * @param expressions the aggregation functions desired
     * @param inputOperator the input op that is feeding the aggregate operator
     * @param context optimization context
     * @param sourceLocation source location
     * @return an aggregate operator with the specified information
     * @throws AlgebricksException when there is error setting the type environment of the newly created aggregate op
     */
    private static AggregateOperator createAggregate(List<LogicalVariable> resultVariables, boolean isGlobal,
            List<Mutable<ILogicalExpression>> expressions, MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        AggregateOperator aggregateOperator = new AggregateOperator(resultVariables, expressions);
        aggregateOperator.setPhysicalOperator(new AggregatePOperator());
        aggregateOperator.setSourceLocation(sourceLocation);
        aggregateOperator.getInputs().add(inputOperator);
        aggregateOperator.setGlobal(isGlobal);
        if (!isGlobal) {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        } else {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        }
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        return aggregateOperator;
    }

    private static ExchangeOperator createRandomMergeExchangeOp(MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new RandomMergeExchangePOperator());
        exchangeOperator.getInputs().add(inputOperator);
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createOneToOneExchangeOp(MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        exchangeOperator.getInputs().add(inputOperator);
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    public static boolean attachRangeMapIfNeeded(ILogicalOperator rootOrderByOp, IOptimizationContext optCtx)
            throws AlgebricksException {
        if (rootOrderByOp == null) {
            return false;
        }
        AbstractLogicalOperator orderAbs = (AbstractLogicalOperator) rootOrderByOp;

        OrderOperator rootOrd = (OrderOperator) orderAbs;
        List<OrderColumn> partitioningColumns = new ArrayList<>();
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : rootOrd.getOrderExpressions()) {
            ILogicalExpression e = p.second.getValue();
            if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
                partitioningColumns.add(new OrderColumn(v, p.first.getKind()));
            }
        }
        RangeMap rm = RangeMapUtil.rangeSplitHelperFunction(optCtx, rootOrd.getInputs().get(0).getValue(),
                partitioningColumns);
        if (rm != null) {
            orderAbs.getAnnotations().put(OperatorAnnotations.USE_STATIC_RANGE, rm);
            return true;
        } else {
            // we tried and decided not to use a static range map
            return false;
        }
    }

    private static RangeMap rangeSplitHelperFunction(IOptimizationContext ctx, ILogicalOperator logicalOperator,
            List<OrderColumn> partitionFields) throws AlgebricksException {

        IOptimizationContext newCtx = ctx.getOptimizationContextFactory().cloneOptimizationContext(ctx);
        // copySchema is false because we will re-compute schema later
        ILogicalOperator newRoot = OperatorManipulationUtil.bottomUpCopyOperators(logicalOperator);
        replaceAllScansWithSamples(newRoot, newCtx);
        OperatorPropertiesUtil.typeOpRec(new MutableObject<>(newRoot), newCtx);

        List<LogicalVariable> outVars = new ArrayList<>(partitionFields.size());
        for (OrderColumn partitionField : partitionFields) {
            outVars.add(partitionField.getColumn());
        }

        ProjectOperator projOp = new ProjectOperator(outVars);
        projOp.getInputs().add(new MutableObject<>(newRoot));
        projOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        OperatorPropertiesUtil.typeOpRec(new MutableObject<>(projOp), newCtx);

        ExchangeOperator exchToLocalAgg = RangeMapUtil.createOneToOneExchangeOp(new MutableObject<>(projOp), newCtx);

        List<LogicalVariable> localVars = new ArrayList<>();
        List<LogicalVariable> rangeMapResultVar = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> localFuns = new ArrayList<>();
        List<Mutable<ILogicalExpression>> rangeMapFun = new ArrayList<>(1);
        SourceLocation srcLoc = projOp.getSourceLocation();

        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);
        OperatorPropertiesUtil.typeOpRec(new MutableObject<>(exchToLocalAgg), newCtx);

        RangeMapUtil.createAggregateFunction(newCtx, localVars, localFuns, rangeMapResultVar, rangeMapFun,
                newCtx.getComputationNodeDomain().cardinality(), partitionFields, srcLoc);
        AggregateOperator localAggOp =
                RangeMapUtil.createAggregate(localVars, false, localFuns, exchToLocalAggRef, newCtx, srcLoc);
        localAggOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(localAggOp);
        OperatorPropertiesUtil.typeOpRec(localAgg, newCtx);

        ExchangeOperator exchToGlobalAgg = RangeMapUtil.createRandomMergeExchangeOp(localAgg, newCtx);
        MutableObject<ILogicalOperator> exchToGlobalAggRef = new MutableObject<>(exchToGlobalAgg);
        OperatorPropertiesUtil.typeOpRec(new MutableObject<>(exchToGlobalAgg), newCtx);

        AggregateOperator globalAggOp =
                RangeMapUtil.createAggregate(rangeMapResultVar, true, rangeMapFun, exchToGlobalAggRef, newCtx, srcLoc);
        OperatorPropertiesUtil.typeOpRec(new MutableObject<>(globalAggOp), newCtx);

        Mutable<ILogicalOperator> topRef = new MutableObject<>(globalAggOp);

        List<List<IAObject>> val;
        try {
            val = AnalysisUtil.runQuery(topRef, rangeMapResultVar, newCtx, IRuleSetFactory.RuleSetKind.SAMPLING);
            return org.apache.asterix.optimizer.rules.cbo.RangeMapUtil.deserializeRangeMap(val);
        } catch (Throwable t) {
            LOGGER.warn("Failed to compute static RangeMap, falling back to stable sort", t);
            return null;
        }
    }

    private static void replaceAllScansWithSamples(ILogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        // replace every DataSourceScan with its SampleDataSource if available.
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scan = (DataSourceScanOperator) op;
            scan.setDataSource(OperatorUtils.getSampleDataSource(scan, ctx)); // scan on sample source
        }
        for (Mutable<ILogicalOperator> in : op.getInputs()) {
            replaceAllScansWithSamples(in.getValue(), ctx);
        }
    }

    private static RangeMap deserializeRangeMap(List<List<IAObject>> val) throws AlgebricksException {
        if (val.get(0).get(0).getType().getTypeTag() != ATypeTag.BINARY) {
            return null;
        }

        ABinary ab = ((ABinary) val.get(0).get(0));
        ByteArrayInputStream bais = new ByteArrayInputStream(ab.getBytes(), ab.getStart(), ab.getLength());
        DataInputStream in = new DataInputStream(bais);

        try {
            int numFields = IntegerSerializerDeserializer.read(in);
            byte[] splitValues = ByteArraySerializerDeserializer.read(in);
            int[] splitValuesEndOffsets = IntArraySerializerDeserializer.read(in);
            double[] percentages = DoubleArraySerializerDeserializer.read(in);

            // build RangeMap
            return new RangeMap(numFields, splitValues, splitValuesEndOffsets, percentages);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

}
