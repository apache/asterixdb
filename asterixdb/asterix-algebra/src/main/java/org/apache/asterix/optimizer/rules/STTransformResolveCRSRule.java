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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CoordinateReferenceSystem;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Resolves the two CRS WKT definitions referenced by an ST_Transform call at compile time
 * from metadata and stores them as opaque parameters on the function expression. The
 * runtime type-inferer then simply forwards these WKT strings to the descriptor. This
 * mirrors {@link FullTextContainsParameterCheckAndSetRule}.
 */
public class STTransformResolveCRSRule implements IAlgebraicRewriteRule {

    private final STTransformExpressionVisitor visitor = new STTransformExpressionVisitor();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        visitor.setContext(context);
        boolean modified = op.acceptExpressionTransform(visitor);
        if (modified) {
            context.addToDontApplySet(this, op);
            OperatorPropertiesUtil.typeOpRec(opRef, context);
            return true;
        }
        return false;
    }

    private static final class STTransformExpressionVisitor implements ILogicalExpressionReferenceTransform {

        private IOptimizationContext context;

        void setContext(IOptimizationContext context) {
            this.context = context;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression e = exprRef.getValue();
            if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            return transformFunctionCall((AbstractFunctionCallExpression) e);
        }

        private boolean transformFunctionCall(AbstractFunctionCallExpression fce) throws AlgebricksException {
            FunctionIdentifier fi = fce.getFunctionIdentifier();
            boolean modified = false;
            if (fi == BuiltinFunctions.ST_TRANSFORM) {
                if (fce.getOpaqueParameters() == null) {
                    resolveCRS(fce);
                    modified = true;
                }
            }
            for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                if (transform(arg)) {
                    modified = true;
                }
            }
            return modified;
        }

        private void resolveCRS(AbstractFunctionCallExpression fce) throws AlgebricksException {
            Long fromSRIDLong = getIntegerLikeConstant(fce.getArguments().get(1).getValue());
            Long toSRIDLong = getIntegerLikeConstant(fce.getArguments().get(2).getValue());

            if (fromSRIDLong == null || toSRIDLong == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fce.getSourceLocation(),
                        "ST_Transform requires constant SRID arguments");
            }
            if (fromSRIDLong < 1 || fromSRIDLong > Integer.MAX_VALUE) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fce.getSourceLocation(),
                        "SRID must be a positive integer (1 to " + Integer.MAX_VALUE + "), got: " + fromSRIDLong);
            }
            if (toSRIDLong < 1 || toSRIDLong > Integer.MAX_VALUE) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fce.getSourceLocation(),
                        "SRID must be a positive integer (1 to " + Integer.MAX_VALUE + "), got: " + toSRIDLong);
            }

            int fromSRID = fromSRIDLong.intValue();
            int toSRID = toSRIDLong.intValue();

            MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
            Namespace defaultNamespace = metadataProvider.getDefaultNamespace();
            String database = defaultNamespace.getDatabaseName();
            DataverseName dataverseName = defaultNamespace.getDataverseName();
            MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();

            CoordinateReferenceSystem fromCrs =
                    MetadataManager.INSTANCE.getCRS(mdTxnCtx, database, dataverseName, fromSRID);
            if (fromCrs == null) {
                throw new CompilationException(ErrorCode.CRS_NOT_FOUND, fce.getSourceLocation(), fromSRID);
            }
            CoordinateReferenceSystem toCrs =
                    MetadataManager.INSTANCE.getCRS(mdTxnCtx, database, dataverseName, toSRID);
            if (toCrs == null) {
                throw new CompilationException(ErrorCode.CRS_NOT_FOUND, fce.getSourceLocation(), toSRID);
            }

            fce.setOpaqueParameters(new Object[] { fromCrs.getCrsWkt(), toCrs.getCrsWkt() });
        }

        private static Long getIntegerLikeConstant(ILogicalExpression expr) {
            Long longConstant = ConstantExpressionUtil.getLongConstant(expr);
            if (longConstant != null) {
                return longConstant;
            }
            Integer intConstant = ConstantExpressionUtil.getIntConstant(expr);
            return intConstant != null ? intConstant.longValue() : null;
        }
    }
}
