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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.IMutationDataSource;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule rewrites all meta() function calls in a query plan
 * to proper variable references.
 */
public class MetaFunctionToMetaVariableRule implements IAlgebraicRewriteRule {
    // The rule can only apply once.
    private boolean hasApplied = false;
    private boolean rewritten = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (hasApplied) {
            return false;
        }
        hasApplied = true;
        visit(opRef);
        return rewritten;
    }

    private ILogicalExpressionReferenceTransformWithCondition visit(Mutable<ILogicalOperator> opRef)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();

        // Reaches NTS or ETS.
        if (op.getInputs().size() == 0) {
            return NoOpExpressionReferenceTransform.INSTANCE;
        }
        // Datascan returns an useful transform if the meta part presents in the dataset.
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scanOp = (DataSourceScanOperator) op;
            ILogicalExpressionReferenceTransformWithCondition inputTransfomer = visit(op.getInputs().get(0));
            AqlDataSource dataSource = (AqlDataSource) scanOp.getDataSource();
            List<ILogicalExpressionReferenceTransformWithCondition> transformers = null;
            List<LogicalVariable> allVars = scanOp.getVariables();
            LogicalVariable dataVar = dataSource.getDataRecordVariable(allVars);
            LogicalVariable metaVar = dataSource.getMetaVariable(allVars);
            LogicalExpressionReferenceTransform currentTransformer = null;
            // https://issues.apache.org/jira/browse/ASTERIXDB-1618
            if (dataSource.getDatasourceType() != AqlDataSourceType.EXTERNAL_DATASET
                    && dataSource.getDatasourceType() != AqlDataSourceType.INTERNAL_DATASET
                    && dataSource.getDatasourceType() != AqlDataSourceType.LOADABLE) {
                IMutationDataSource mds = (IMutationDataSource) dataSource;
                if (mds.isChange()) {
                    transformers = new ArrayList<>();
                    transformers.add(new MetaKeyExpressionReferenceTransform(mds.getPkVars(allVars),
                            mds.getKeyAccessExpression()));
                } else if (metaVar != null) {
                    transformers = new ArrayList<>();
                    transformers.add(new MetaKeyToFieldAccessTransform(metaVar));
                }
            }
            if (!dataSource.hasMeta() && transformers == null) {
                return inputTransfomer;
            }
            if (metaVar != null) {
                currentTransformer = new LogicalExpressionReferenceTransform(dataVar, metaVar);
            }
            if (inputTransfomer.equals(NoOpExpressionReferenceTransform.INSTANCE) && transformers == null) {
                return currentTransformer;
            } else if (inputTransfomer.equals(NoOpExpressionReferenceTransform.INSTANCE)
                    && currentTransformer == null) {
                return transformers.get(0);
            } else {
                // Requires an argument variable to resolve ambiguity.
                if (transformers == null) {
                    transformers = new ArrayList<>();
                }
                if (!inputTransfomer.equals(NoOpExpressionReferenceTransform.INSTANCE)) {
                    inputTransfomer.setVariableRequired();
                    transformers.add(inputTransfomer);
                }
                currentTransformer.setVariableRequired();
                transformers.add(currentTransformer);
                return new CompositeExpressionReferenceTransform(transformers);
            }
        }

        // Visits children in the depth-first order.
        List<ILogicalExpressionReferenceTransformWithCondition> transformers = new ArrayList<>();
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            ILogicalExpressionReferenceTransformWithCondition transformer = visit(childRef);
            if (!transformer.equals(NoOpExpressionReferenceTransform.INSTANCE)) {
                transformers.add(transformer);
            }
        }
        ILogicalExpressionReferenceTransformWithCondition currentTransformer = null;
        if (transformers.size() == 0) {
            currentTransformer = NoOpExpressionReferenceTransform.INSTANCE;
        } else if (transformers.size() == 1) {
            currentTransformer = transformers.get(0);
        } else {
            // Transformers in a CompositeTransformer should require argument variable check.
            for (ILogicalExpressionReferenceTransformWithCondition transformer : transformers) {
                transformer.setVariableRequired();
            }
            currentTransformer = new CompositeExpressionReferenceTransform(transformers);
        }
        rewritten |= op.acceptExpressionTransform(currentTransformer);
        return currentTransformer;
    }
}

interface ILogicalExpressionReferenceTransformWithCondition extends ILogicalExpressionReferenceTransform {
    default void setVariableRequired() {

    }
}

class NoOpExpressionReferenceTransform implements ILogicalExpressionReferenceTransformWithCondition {
    static final NoOpExpressionReferenceTransform INSTANCE = new NoOpExpressionReferenceTransform();

    private NoOpExpressionReferenceTransform() {

    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> expression) throws AlgebricksException {
        return false;
    }

}

class LogicalExpressionReferenceTransform implements ILogicalExpressionReferenceTransformWithCondition {
    private final LogicalVariable dataVar;
    private final LogicalVariable metaVar;
    private boolean variableRequired = false;

    LogicalExpressionReferenceTransform(LogicalVariable dataVar, LogicalVariable metaVar) {
        this.dataVar = dataVar;
        this.metaVar = metaVar;
    }

    @Override
    public void setVariableRequired() {
        this.variableRequired = true;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        List<Mutable<ILogicalExpression>> argRefs = funcExpr.getArguments();

        // Recursively transform argument expressions.
        for (Mutable<ILogicalExpression> argRef : argRefs) {
            transform(argRef);
        }

        if (!funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.META)) {
            return false;
        }
        // The user query provides more than one parameter for the meta function.
        if (argRefs.size() > 1) {
            throw new AlgebricksException("The meta function can at most have one argument!");
        }

        // The user query provides exact one parameter for the meta function.
        if (argRefs.size() == 1) {
            ILogicalExpression argExpr = argRefs.get(0).getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return false;
            }
            VariableReferenceExpression argVarExpr = (VariableReferenceExpression) argExpr;
            LogicalVariable argVar = argVarExpr.getVariableReference();
            if (!dataVar.equals(argVar)) {
                return false;
            }
            exprRef.setValue(new VariableReferenceExpression(metaVar));
            return true;
        }

        // The user query provides zero parameter for the meta function.
        if (variableRequired) {
            throw new AlgebricksException("Cannot resolve to ambiguity on the meta function call --"
                    + " there are more than once dataset choices!");
        }
        exprRef.setValue(new VariableReferenceExpression(metaVar));
        return true;
    }
}

class CompositeExpressionReferenceTransform implements ILogicalExpressionReferenceTransformWithCondition {
    private final List<ILogicalExpressionReferenceTransformWithCondition> transformers;

    public CompositeExpressionReferenceTransform(List<ILogicalExpressionReferenceTransformWithCondition> transforms) {
        this.transformers = transforms;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> expression) throws AlgebricksException {
        // Tries transfomations one by one.
        for (ILogicalExpressionReferenceTransform transformer : transformers) {
            if (transformer.transform(expression)) {
                return true;
            }
        }
        return false;
    }
}

class MetaKeyToFieldAccessTransform implements ILogicalExpressionReferenceTransformWithCondition {
    private final LogicalVariable metaVar;

    MetaKeyToFieldAccessTransform(LogicalVariable recordVar) {
        this.metaVar = recordVar;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.META_KEY)) {
            return false;
        }
        // Get arguments
        // first argument : Resource key
        // second argument: field
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        ConstantExpression fieldNameExpression = (ConstantExpression) args.get(1).getValue();
        AsterixConstantValue fieldNameValue = (AsterixConstantValue) fieldNameExpression.getValue();
        IAType fieldNameType = fieldNameValue.getObject().getType();
        FunctionIdentifier functionIdentifier;
        switch (fieldNameType.getTypeTag()) {
            case ORDEREDLIST:
                // Field access nested
                functionIdentifier = AsterixBuiltinFunctions.FIELD_ACCESS_NESTED;
                break;
            case STRING:
                // field access by name
                functionIdentifier = AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME;
                break;
            default:
                throw new AlgebricksException("Unsupported field name type " + fieldNameType.getTypeTag());
        }
        IFunctionInfo finfoAccess = FunctionUtil.getFunctionInfo(functionIdentifier);
        ArrayList<Mutable<ILogicalExpression>> argExprs = new ArrayList<>(2);
        argExprs.add(new MutableObject<>(new VariableReferenceExpression(metaVar)));
        argExprs.add(new MutableObject<>(fieldNameExpression));
        exprRef.setValue(new ScalarFunctionCallExpression(finfoAccess, argExprs));
        return true;
    }
}

class MetaKeyExpressionReferenceTransform implements ILogicalExpressionReferenceTransformWithCondition {
    private final List<LogicalVariable> keyVars;
    private final List<ScalarFunctionCallExpression> metaKeyAccessExpressions;

    MetaKeyExpressionReferenceTransform(List<LogicalVariable> keyVars,
            List<ScalarFunctionCallExpression> metaKeyAccessExpressions) {
        this.keyVars = keyVars;
        this.metaKeyAccessExpressions = metaKeyAccessExpressions;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.META_KEY)) {
            return false;
        }

        // Function is meta key access
        for (int i = 0; i < metaKeyAccessExpressions.size(); i++) {
            if (metaKeyAccessExpressions.get(i).equals(funcExpr)) {
                exprRef.setValue(new VariableReferenceExpression(keyVars.get(i)));
                return true;
            }
        }
        return false;
    }
}
