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

package org.apache.asterix.optimizer.rules;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.dataflow.data.common.ExpressionTypeComputer;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.formats.nontagged.ADMPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFamilyProvider;
import org.apache.asterix.formats.nontagged.BinaryIntegerInspector;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.jobgen.QueryLogicalExpressionJobGen;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

import com.google.common.collect.ImmutableMap;

public class ConstantFoldingRule implements IAlgebraicRewriteRule {

    private final ConstantFoldingVisitor cfv = new ConstantFoldingVisitor();
    private final JobGenContext jobGenCtx;

    private static final Map<FunctionIdentifier, IAObject> FUNC_ID_TO_CONSTANT = ImmutableMap
            .of(BuiltinFunctions.NUMERIC_E, new ADouble(Math.E), BuiltinFunctions.NUMERIC_PI, new ADouble(Math.PI));

    /**
     * Throws exceptions in substituteProducedVariable, setVarType, and one getVarType method.
     */
    private static final IVariableTypeEnvironment _emptyTypeEnv = new IVariableTypeEnvironment() {

        @Override
        public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) {
            throw new IllegalStateException();
        }

        @Override
        public void setVarType(LogicalVariable var, Object type) {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
                List<List<LogicalVariable>> correlatedNullableVariableLists) {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var) {
            throw new IllegalStateException();
        }

        @Override
        public Object getType(ILogicalExpression expr) throws AlgebricksException {
            return ExpressionTypeComputer.INSTANCE.getType(expr, null, this);
        }
    };

    private static final IOperatorSchema[] _emptySchemas = new IOperatorSchema[] {};

    public ConstantFoldingRule(ICcApplicationContext appCtx) {
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        jobGenCtx = new JobGenContext(null, metadataProvider, appCtx, SerializerDeserializerProvider.INSTANCE,
                BinaryHashFunctionFactoryProvider.INSTANCE, BinaryHashFunctionFamilyProvider.INSTANCE,
                BinaryComparatorFactoryProvider.INSTANCE, TypeTraitProvider.INSTANCE, BinaryBooleanInspector.FACTORY,
                BinaryIntegerInspector.FACTORY, ADMPrinterFactoryProvider.INSTANCE, MissingWriterFactory.INSTANCE, null,
                new ExpressionRuntimeProvider(new QueryLogicalExpressionJobGen(metadataProvider.getFunctionManager())),
                ExpressionTypeComputer.INSTANCE, null, null, null, null, GlobalConfig.DEFAULT_FRAME_SIZE, null, 0);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        cfv.reset(context);
        return op.acceptExpressionTransform(cfv);
    }

    private class ConstantFoldingVisitor implements ILogicalExpressionVisitor<Pair<Boolean, ILogicalExpression>, Void>,
            ILogicalExpressionReferenceTransform {

        private final IPointable p = VoidPointable.FACTORY.createPointable();
        private final ByteBufferInputStream bbis = new ByteBufferInputStream();
        private final DataInputStream dis = new DataInputStream(bbis);
        private final WarningCollector warningCollector = new WarningCollector();
        private final IEvaluatorContext evalContext = new EvaluatorContext(warningCollector);
        private IOptimizationContext optContext;

        private void reset(IOptimizationContext context) {
            optContext = context;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            Pair<Boolean, ILogicalExpression> newExpression = expr.accept(this, null);
            if (newExpression.first) {
                exprRef.setValue(newExpression.second);
            }
            return newExpression.first;
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitConstantExpression(ConstantExpression expr, Void arg) {
            return new Pair<>(false, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitVariableReferenceExpression(VariableReferenceExpression expr,
                Void arg) {
            return new Pair<>(false, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr,
                Void arg) throws AlgebricksException {
            boolean changed = constantFoldArgs(expr, arg);
            if (!allArgsConstant(expr) || !expr.isFunctional() || !canConstantFold(expr)) {
                return new Pair<>(changed, expr);
            }

            try {
                if (expr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                    ARecordType rt = (ARecordType) _emptyTypeEnv.getType(expr.getArguments().get(0).getValue());
                    String str = ConstantExpressionUtil.getStringConstant(expr.getArguments().get(1).getValue());
                    int k = rt.getFieldIndex(str);
                    if (k >= 0) {
                        // wait for the ByNameToByIndex rule to apply
                        return new Pair<>(changed, expr);
                    }
                }
                IAObject c = FUNC_ID_TO_CONSTANT.get(expr.getFunctionIdentifier());
                if (c != null) {
                    ConstantExpression constantExpression = new ConstantExpression(new AsterixConstantValue(c));
                    constantExpression.setSourceLocation(expr.getSourceLocation());
                    return new Pair<>(true, constantExpression);
                }

                IScalarEvaluatorFactory fact = jobGenCtx.getExpressionRuntimeProvider().createEvaluatorFactory(expr,
                        _emptyTypeEnv, _emptySchemas, jobGenCtx);

                warningCollector.clear();
                IScalarEvaluator eval = fact.createScalarEvaluator(evalContext);
                eval.evaluate(null, p);
                IAType returnType = (IAType) _emptyTypeEnv.getType(expr);
                ATypeTag runtimeType = PointableHelper.getTypeTag(p);
                if (runtimeType.isDerivedType()) {
                    returnType = TypeComputeUtils.getActualType(returnType);
                } else {
                    returnType = TypeTagUtil.getBuiltinTypeByTag(runtimeType);
                }
                @SuppressWarnings("rawtypes")
                ISerializerDeserializer serde =
                        jobGenCtx.getSerializerDeserializerProvider().getSerializerDeserializer(returnType);
                bbis.setByteBuffer(ByteBuffer.wrap(p.getByteArray(), p.getStartOffset(), p.getLength()), 0);
                IAObject o = (IAObject) serde.deserialize(dis);
                warningCollector.getWarnings(optContext.getWarningCollector());
                ConstantExpression constantExpression = new ConstantExpression(new AsterixConstantValue(o));
                constantExpression.setSourceLocation(expr.getSourceLocation());
                return new Pair<>(true, constantExpression);
            } catch (HyracksDataException | AlgebricksException e) {
                if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Exception caught at constant folding: " + e, e);
                }
                return new Pair<>(false, null);
            }
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitAggregateFunctionCallExpression(
                AggregateFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = constantFoldArgs(expr, arg);
            return new Pair<>(changed, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitStatefulFunctionCallExpression(
                StatefulFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = constantFoldArgs(expr, arg);
            return new Pair<>(changed, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitUnnestingFunctionCallExpression(
                UnnestingFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = constantFoldArgs(expr, arg);
            return new Pair<>(changed, expr);
        }

        private boolean constantFoldArgs(AbstractFunctionCallExpression expr, Void arg) throws AlgebricksException {
            return expr.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                    ? foldRecordArgs(expr, arg) : foldFunctionArgs(expr, arg);
        }

        private boolean foldFunctionArgs(AbstractFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = false;
            for (Mutable<ILogicalExpression> exprArgRef : expr.getArguments()) {
                changed |= foldArg(exprArgRef, arg);
            }
            return changed;
        }

        private boolean foldRecordArgs(AbstractFunctionCallExpression expr, Void arg) throws AlgebricksException {
            if (expr.getArguments().size() % 2 != 0) {
                String functionName = expr.getFunctionIdentifier().getName();
                throw CompilationException.create(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                        functionName);
            }
            boolean changed = false;
            Iterator<Mutable<ILogicalExpression>> iterator = expr.getArguments().iterator();
            int fieldNameIdx = 0;
            while (iterator.hasNext()) {
                Mutable<ILogicalExpression> fieldNameExprRef = iterator.next();
                Pair<Boolean, ILogicalExpression> fieldNameExpr = fieldNameExprRef.getValue().accept(this, arg);
                boolean isDuplicate = false;
                if (fieldNameExpr.first) {
                    String fieldName = ConstantExpressionUtil.getStringConstant(fieldNameExpr.second);
                    if (fieldName != null) {
                        isDuplicate = isDuplicateField(fieldName, fieldNameIdx, expr.getArguments());
                    }
                    if (isDuplicate) {
                        IWarningCollector warningCollector = optContext.getWarningCollector();
                        if (warningCollector.shouldWarn()) {
                            warningCollector.warn(WarningUtil.forAsterix(fieldNameExpr.second.getSourceLocation(),
                                    ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, fieldName));
                        }
                        iterator.remove();
                        iterator.next();
                        iterator.remove();
                    } else {
                        fieldNameExprRef.setValue(fieldNameExpr.second);
                    }
                    changed = true;
                }
                if (!isDuplicate) {
                    Mutable<ILogicalExpression> fieldValue = iterator.next();
                    changed |= foldArg(fieldValue, arg);
                    fieldNameIdx += 2;
                }
            }
            return changed;
        }

        private boolean isDuplicateField(String fName, int fIdx, List<Mutable<ILogicalExpression>> args) {
            for (int i = 0, size = args.size(); i < size; i += 2) {
                if (i != fIdx && fName.equals(ConstantExpressionUtil.getStringConstant(args.get(i).getValue()))) {
                    return true;
                }
            }
            return false;
        }

        private boolean foldArg(Mutable<ILogicalExpression> exprArgRef, Void arg) throws AlgebricksException {
            Pair<Boolean, ILogicalExpression> newExpr = exprArgRef.getValue().accept(this, arg);
            if (newExpr.first) {
                exprArgRef.setValue(newExpr.second);
                return true;
            }
            return false;
        }

        private boolean allArgsConstant(AbstractFunctionCallExpression expr) {
            for (Mutable<ILogicalExpression> r : expr.getArguments()) {
                if (r.getValue().getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return false;
                }
            }
            return true;
        }

        private boolean canConstantFold(ScalarFunctionCallExpression function) throws AlgebricksException {
            // skip all functions that would produce records/arrays/multisets (derived types) in their open format
            // this is because constant folding them will make them closed (currently)
            if (function.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
                return false;
            }
            IAType returnType = (IAType) _emptyTypeEnv.getType(function);
            return canConstantFoldType(returnType);
        }

        private boolean canConstantFoldType(IAType returnType) {
            ATypeTag tag = returnType.getTypeTag();
            if (tag == ATypeTag.ANY) {
                // if the function is to return a record (or derived data), that record would (should) be an open record
                return false;
            } else if (tag == ATypeTag.OBJECT) {
                ARecordType recordType = (ARecordType) returnType;
                if (recordType.isOpen()) {
                    return false;
                }
                IAType[] fieldTypes = recordType.getFieldTypes();
                for (int i = 0; i < fieldTypes.length; i++) {
                    if (!canConstantFoldType(fieldTypes[i])) {
                        return false;
                    }
                }
            } else if (tag.isListType()) {
                AbstractCollectionType listType = (AbstractCollectionType) returnType;
                return canConstantFoldType(listType.getItemType());
            } else if (tag == ATypeTag.UNION) {
                return canConstantFoldType(((AUnionType) returnType).getActualType());
            }
            return true;
        }
    }
}
