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
package org.apache.asterix.optimizer.base;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.ResultSetDataSink;
import org.apache.asterix.metadata.declared.ResultSetSinkId;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.translator.ResultMetadata;
import org.apache.asterix.translator.SessionConfig;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.compiler.api.ICompiler;
import org.apache.hyracks.algebricks.compiler.api.ICompilerFactory;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class AnalysisUtil {

    private static final List<FunctionIdentifier> fieldAccessFunctions =
            Arrays.asList(BuiltinFunctions.GET_DATA, BuiltinFunctions.GET_HANDLE, BuiltinFunctions.TYPE_OF);

    /*
     * If the first child of op is of type opType, then it returns that child,
     * o/w returns null.
     */
    public final static ILogicalOperator firstChildOfType(AbstractLogicalOperator op, LogicalOperatorTag opType) {
        List<Mutable<ILogicalOperator>> ins = op.getInputs();
        if (ins == null || ins.isEmpty()) {
            return null;
        }
        Mutable<ILogicalOperator> opRef2 = ins.get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() == opType) {
            return op2;
        } else {
            return null;
        }
    }

    public static int numberOfVarsInExpr(ILogicalExpression e) {
        switch (e.getExpressionTag()) {
            case CONSTANT: {
                return 0;
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) e;
                int s = 0;
                for (Mutable<ILogicalExpression> arg : f.getArguments()) {
                    s += numberOfVarsInExpr(arg.getValue());
                }
                return s;
            }
            case VARIABLE: {
                return 1;
            }
            default: {
                assert false;
                throw new IllegalArgumentException();
            }
        }
    }

    public static boolean isRunnableFieldAccessFunction(FunctionIdentifier fid) {
        return fieldAccessFunctions.contains(fid);
    }

    public static boolean isRunnableAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (AnalysisUtil.isRunnableFieldAccessFunction(fid)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessByNameToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX) || fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || fid.equals(BuiltinFunctions.FIELD_ACCESS_NESTED)) {
                return true;
            }
        }
        return false;
    }

    public static Triple<DataverseName, String, String> getExternalDatasetInfo(UnnestMapOperator op)
            throws AlgebricksException {
        AbstractFunctionCallExpression unnestExpr = (AbstractFunctionCallExpression) op.getExpressionRef().getValue();
        String databaseName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(0));
        DataverseName dataverseName = DataverseName
                .createFromCanonicalForm(AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(1)));
        String datasetName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(2));
        return new Triple<>(dataverseName, datasetName, databaseName);
    }

    /**
     * Checks whether a window operator has a function call where the function has given property
     */
    public static boolean hasFunctionWithProperty(WindowOperator winOp,
            BuiltinFunctions.WindowFunctionProperty property) throws CompilationException {
        for (Mutable<ILogicalExpression> exprRef : winOp.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, winOp.getSourceLocation(),
                        expr.getExpressionTag());
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (BuiltinFunctions.builtinFunctionHasProperty(callExpr.getFunctionIdentifier(), property)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether frame boundary expression is a monotonically non-descreasing function over a frame value variable
     */
    public static boolean isWindowFrameBoundaryMonotonic(List<Mutable<ILogicalExpression>> frameBoundaryExprList,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExprList) {
        if (frameValueExprList.size() != 1) {
            return false;
        }
        ILogicalExpression frameValueExpr = frameValueExprList.get(0).second.getValue();
        if (frameValueExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (frameBoundaryExprList.size() != 1) {
            return false;
        }
        ILogicalExpression frameStartExpr = frameBoundaryExprList.get(0).getValue();
        switch (frameStartExpr.getExpressionTag()) {
            case CONSTANT:
                return true;
            case VARIABLE:
                return frameStartExpr.equals(frameValueExpr);
            case FUNCTION_CALL:
                AbstractFunctionCallExpression frameStartCallExpr = (AbstractFunctionCallExpression) frameStartExpr;
                FunctionIdentifier fi = frameStartCallExpr.getFunctionIdentifier();
                return (BuiltinFunctions.NUMERIC_ADD.equals(fi) || BuiltinFunctions.NUMERIC_SUBTRACT.equals(fi))
                        && frameStartCallExpr.getArguments().get(0).getValue().equals(frameValueExpr)
                        && frameStartCallExpr.getArguments().get(1).getValue()
                                .getExpressionTag() == LogicalExpressionTag.CONSTANT;
            default:
                throw new IllegalStateException(String.valueOf(frameStartExpr.getExpressionTag()));
        }
    }

    public static boolean isTrivialAggregateSubplan(ILogicalPlan subplan) {
        if (subplan.getRoots().isEmpty()) {
            return false;
        }
        for (Mutable<ILogicalOperator> rootOpRef : subplan.getRoots()) {
            ILogicalOperator rootOp = rootOpRef.getValue();
            if (rootOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                return false;
            }
            if (firstChildOfType((AbstractLogicalOperator) rootOp, LogicalOperatorTag.NESTEDTUPLESOURCE) == null) {
                return false;
            }
        }
        return true;
    }

    public static List<List<IAObject>> runQuery(Mutable<ILogicalOperator> topOp, List<LogicalVariable> resultVars,
            IOptimizationContext queryOptCtx, IRuleSetKind ruleSetKind) throws AlgebricksException {

        MetadataProvider metadataProvider = (MetadataProvider) queryOptCtx.getMetadataProvider();
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        TxnId mainTxnId = metadataProvider.getTxnId();
        try {
            TxnId newTxnId = metadataProvider.getTxnIdFactory().create();
            metadataProvider.setTxnId(newTxnId);

            IVariableTypeEnvironment topOpTypeEnv = queryOptCtx.getOutputTypeEnvironment(topOp.getValue());
            SerializerDeserializerProvider serdeProvider = SerializerDeserializerProvider.INSTANCE;

            int nFields = resultVars.size();
            List<Mutable<ILogicalExpression>> resultExprList = new ArrayList<>(nFields);
            List<ISerializerDeserializer<?>> resultSerdeList = new ArrayList<>(nFields);

            for (LogicalVariable var : resultVars) {
                Object varType = topOpTypeEnv.getVarType(var);
                if (varType == null) {
                    throw new IllegalArgumentException("Cannot determine type of " + var);
                }
                resultSerdeList.add(serdeProvider.getSerializerDeserializer(varType));
                resultExprList.add(new MutableObject<>(new VariableReferenceExpression(var)));
            }

            ResultMetadata resultMetadata = new ResultMetadata(SessionConfig.OutputFormat.ADM);
            ResultSetId resultSetId = new ResultSetId(metadataProvider.getResultSetIdCounter().getAndInc());
            ResultSetSinkId rssId = new ResultSetSinkId(resultSetId);
            ResultSetDataSink sink = new ResultSetDataSink(rssId, null);

            DistributeResultOperator resultOp = new DistributeResultOperator(resultExprList, sink, resultMetadata);
            resultOp.getInputs().add(topOp);
            queryOptCtx.computeAndSetTypeEnvironmentForOperator(resultOp);

            MutableObject<ILogicalOperator> newResultOpRef = new MutableObject<>(resultOp);

            ICompilerFactory compilerFactory = (ICompilerFactory) queryOptCtx.getCompilerFactory();
            ICompiler compiler =
                    compilerFactory.createCompiler(new ALogicalPlanImpl(newResultOpRef), queryOptCtx, ruleSetKind);
            compiler.optimize();

            JobSpecification jobSpec = compiler.createJob(appCtx, new JobEventListenerFactory(newTxnId, false));

            JobId jobId = JobUtils.runJob(appCtx.getHcc(), jobSpec, true);

            IResultSetReader resultSetReader = appCtx.getResultSet().createReader(jobId, resultSetId);
            FrameManager frameManager = new FrameManager(queryOptCtx.getPhysicalOptimizationConfig().getFrameSize());
            IFrame frame = new VSizeFrame(frameManager);

            FrameTupleAccessor fta = new FrameTupleAccessor(null);
            ByteArrayAccessibleInputStream bais = new ByteArrayAccessibleInputStream(frame.getBuffer().array(), 0, 0);
            DataInputStream dis = new DataInputStream(bais);
            List<List<IAObject>> result = new ArrayList<>();

            while (resultSetReader.read(frame) > 0) {
                ByteBuffer buffer = frame.getBuffer();
                fta.reset(buffer);
                int nTuples = fta.getTupleCount();
                for (int tupleIdx = 0; tupleIdx < nTuples; tupleIdx++) {
                    int tupleStart = fta.getTupleStartOffset(tupleIdx);
                    int tupleEnd = fta.getTupleEndOffset(tupleIdx);
                    bais.setContent(buffer.array(), tupleStart, tupleEnd - tupleStart);

                    List<IAObject> values = new ArrayList<>(nFields);
                    for (int fieldIdx = 0; fieldIdx < nFields; fieldIdx++) {
                        IAObject value = (IAObject) resultSerdeList.get(fieldIdx).deserialize(dis);
                        values.add(value);
                    }
                    result.add(values);
                }
            }

            return result;
        } catch (AlgebricksException e) {
            throw e;
        } catch (Exception e) {
            throw new AlgebricksException(e);
        } finally {
            metadataProvider.setTxnId(mainTxnId);
        }
    }
}
