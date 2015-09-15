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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import org.apache.asterix.dataflow.data.common.AqlNullableTypeComputer;
import org.apache.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import org.apache.asterix.formats.nontagged.AqlBinaryBooleanInspectorImpl;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFamilyProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryIntegerInspector;
import org.apache.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.jobgen.AqlLogicalExpressionJobGen;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ConstantFoldingRule implements IAlgebraicRewriteRule {

    private final ConstantFoldingVisitor cfv = new ConstantFoldingVisitor();

    /** Throws exceptions in substituiteProducedVariable, setVarType, and one getVarType method. */
    private static final IVariableTypeEnvironment _emptyTypeEnv = new IVariableTypeEnvironment() {

        @Override
        public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
            throw new IllegalStateException();
        }

        @Override
        public void setVarType(LogicalVariable var, Object type) {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
                List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var) throws AlgebricksException {
            throw new IllegalStateException();
        }

        @Override
        public Object getType(ILogicalExpression expr) throws AlgebricksException {
            return AqlExpressionTypeComputer.INSTANCE.getType(expr, null, this);
        }
    };

    private static final JobGenContext _jobGenCtx = new JobGenContext(null, null, null,
            AqlSerializerDeserializerProvider.INSTANCE, AqlBinaryHashFunctionFactoryProvider.INSTANCE,
            AqlBinaryHashFunctionFamilyProvider.INSTANCE, AqlBinaryComparatorFactoryProvider.INSTANCE,
            AqlTypeTraitProvider.INSTANCE, AqlBinaryBooleanInspectorImpl.FACTORY, AqlBinaryIntegerInspector.FACTORY,
            AqlPrinterFactoryProvider.INSTANCE, AqlNullWriterFactory.INSTANCE, null,
            new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter(AqlLogicalExpressionJobGen.INSTANCE),
            AqlExpressionTypeComputer.INSTANCE, AqlNullableTypeComputer.INSTANCE, null, null, null, null,
            GlobalConfig.DEFAULT_FRAME_SIZE, null);

    private static final IOperatorSchema[] _emptySchemas = new IOperatorSchema[] {};

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        return op.acceptExpressionTransform(cfv);
    }

    private class ConstantFoldingVisitor implements ILogicalExpressionVisitor<Pair<Boolean, ILogicalExpression>, Void>,
            ILogicalExpressionReferenceTransform {

        private final IPointable p = VoidPointable.FACTORY.createPointable();
        private final ByteBufferInputStream bbis = new ByteBufferInputStream();
        private final DataInputStream dis = new DataInputStream(bbis);

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            Pair<Boolean, ILogicalExpression> p = expr.accept(this, null);
            if (p.first) {
                exprRef.setValue(p.second);
            }
            return p.first;
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitConstantExpression(ConstantExpression expr, Void arg)
                throws AlgebricksException {
            return new Pair<Boolean, ILogicalExpression>(false, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitVariableReferenceExpression(VariableReferenceExpression expr,
                Void arg) throws AlgebricksException {
            return new Pair<Boolean, ILogicalExpression>(false, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr,
                Void arg) throws AlgebricksException {
            boolean changed = changeRec(expr, arg);
            if (!checkArgs(expr) || !expr.isFunctional()) {
                return new Pair<Boolean, ILogicalExpression>(changed, expr);
            }
            //Current ARecord SerDe assumes a closed record, so we do not constant fold open record constructors
            if (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                    || expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.CAST_RECORD)) {
                return new Pair<Boolean, ILogicalExpression>(false, null);
            }
            //Current List SerDe assumes a strongly typed list, so we do not constant fold the list constructors if they are not strongly typed
            if (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR)
                    || expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)) {
                AbstractCollectionType listType = (AbstractCollectionType) TypeComputerUtilities.getRequiredType(expr);
                if (listType != null
                        && (listType.getItemType().getTypeTag() == ATypeTag.ANY || listType.getItemType() instanceof AbstractCollectionType)) {
                    //case1: listType == null,  could be a nested list inside a list<ANY>
                    //case2: itemType = ANY
                    //case3: itemType = a nested list
                    return new Pair<Boolean, ILogicalExpression>(false, null);
                }
            }
            if (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                ARecordType rt = (ARecordType) _emptyTypeEnv.getType(expr.getArguments().get(0).getValue());
                String str = ((AString) ((AsterixConstantValue) ((ConstantExpression) expr.getArguments().get(1)
                        .getValue()).getValue()).getObject()).getStringValue();
                int k;
                try {
                    k = rt.findFieldPosition(str);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
                if (k >= 0) {
                    // wait for the ByNameToByIndex rule to apply
                    return new Pair<Boolean, ILogicalExpression>(changed, expr);
                }
            }
            IScalarEvaluatorFactory fact = _jobGenCtx.getExpressionRuntimeProvider().createEvaluatorFactory(expr,
                    _emptyTypeEnv, _emptySchemas, _jobGenCtx);
            IScalarEvaluator eval = fact.createScalarEvaluator(null);
            eval.evaluate(null, p);
            Object t = _emptyTypeEnv.getType(expr);

            @SuppressWarnings("rawtypes")
            ISerializerDeserializer serde = _jobGenCtx.getSerializerDeserializerProvider().getSerializerDeserializer(t);
            bbis.setByteBuffer(ByteBuffer.wrap(p.getByteArray(), p.getStartOffset(), p.getLength()), 0);
            IAObject o;
            try {
                o = (IAObject) serde.deserialize(dis);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            return new Pair<Boolean, ILogicalExpression>(true, new ConstantExpression(new AsterixConstantValue(o)));
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitAggregateFunctionCallExpression(
                AggregateFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = changeRec(expr, arg);
            return new Pair<Boolean, ILogicalExpression>(changed, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitStatefulFunctionCallExpression(
                StatefulFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = changeRec(expr, arg);
            return new Pair<Boolean, ILogicalExpression>(changed, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitUnnestingFunctionCallExpression(
                UnnestingFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = changeRec(expr, arg);
            return new Pair<Boolean, ILogicalExpression>(changed, expr);
        }

        private boolean changeRec(AbstractFunctionCallExpression expr, Void arg) throws AlgebricksException {
            boolean changed = false;
            for (Mutable<ILogicalExpression> r : expr.getArguments()) {
                Pair<Boolean, ILogicalExpression> p2 = r.getValue().accept(this, arg);
                if (p2.first) {
                    r.setValue(p2.second);
                    changed = true;
                }
            }
            return changed;
        }

        private boolean checkArgs(AbstractFunctionCallExpression expr) throws AlgebricksException {
            for (Mutable<ILogicalExpression> r : expr.getArguments()) {
                if (r.getValue().getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return false;
                }
            }
            return true;
        }
    }
}
