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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.IMutationDataSource;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule rewrites all meta() and meta-key() function calls in a query plan to proper variable references.
 */
public class MetaFunctionToMetaVariableRule implements IAlgebraicRewriteRule {
    // the rule can only apply once.
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

        // reaches NTS or ETS.
        if (op.getInputs().isEmpty()) {
            return NoOpExpressionReferenceTransform.INSTANCE;
        }
        // datascan returns a useful transform if the meta part is present in the dataset.
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scanOp = (DataSourceScanOperator) op;
            ILogicalExpressionReferenceTransformWithCondition inputTransformer = visit(op.getInputs().get(0));
            DataSource dataSource = (DataSource) scanOp.getDataSource();
            List<ILogicalExpressionReferenceTransformWithCondition> transformers = null;
            List<LogicalVariable> allVars = scanOp.getVariables();
            LogicalVariable dataVar = dataSource.getDataRecordVariable(allVars);
            LogicalVariable metaVar = dataSource.getMetaVariable(allVars);
            LogicalExpressionReferenceTransform currentTransformer = null;
            // https://issues.apache.org/jira/browse/ASTERIXDB-1618
            if (dataSource.getDatasourceType() != DataSource.Type.EXTERNAL_DATASET
                    && dataSource.getDatasourceType() != DataSource.Type.INTERNAL_DATASET
                    && dataSource.getDatasourceType() != DataSource.Type.LOADABLE
                    && dataSource.getDatasourceType() != DataSource.Type.FUNCTION) {
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
                return inputTransformer;
            }
            if (metaVar != null) {
                currentTransformer = new LogicalExpressionReferenceTransform(dataVar, metaVar);
            }
            if (inputTransformer.equals(NoOpExpressionReferenceTransform.INSTANCE) && transformers == null) {
                return currentTransformer;
            } else if (inputTransformer.equals(NoOpExpressionReferenceTransform.INSTANCE)
                    && currentTransformer == null) {
                return transformers.get(0);
            } else {
                if (transformers == null) {
                    transformers = new ArrayList<>();
                }
                if (!inputTransformer.equals(NoOpExpressionReferenceTransform.INSTANCE)) {
                    // require an argument variable to resolve ambiguity when there are 2 or more distinct data sources
                    inputTransformer.setVariableRequired();
                    currentTransformer.setVariableRequired();
                    transformers.add(inputTransformer);
                }
                transformers.add(currentTransformer);
                return new CompositeExpressionReferenceTransform(transformers);
            }
        }

        // visit children in the depth-first order.
        List<ILogicalExpressionReferenceTransformWithCondition> transformers = new ArrayList<>();
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            ILogicalExpressionReferenceTransformWithCondition transformer = visit(childRef);
            if (!transformer.equals(NoOpExpressionReferenceTransform.INSTANCE)) {
                transformers.add(transformer);
            }
        }
        ILogicalExpressionReferenceTransformWithCondition currentTransformer = null;
        if (transformers.isEmpty()) {
            currentTransformer = NoOpExpressionReferenceTransform.INSTANCE;
        } else if (transformers.size() == 1) {
            currentTransformer = transformers.get(0);
        } else {
            // transformers in a CompositeTransformer should require argument variable check.
            for (ILogicalExpressionReferenceTransformWithCondition transformer : transformers) {
                transformer.setVariableRequired();
            }
            currentTransformer = new CompositeExpressionReferenceTransform(transformers);
        }

        if (((AbstractLogicalOperator) op).hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNestedPlans = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan nestedPlan : opWithNestedPlans.getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : nestedPlan.getRoots()) {
                    visit(root);
                }
            }
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
    public boolean transform(Mutable<ILogicalExpression> expression) {
        return false;
    }

}

/**
 * <pre>
 * This class replaces meta() references with their corresponding meta record variables. It maintains the data record
 * variable and meta record variable. The data variable is used to match the data variable inside meta() if supplied.
 * For example:
 * If the data source produces 2 records, the data record as $$ds and the meta record as $$7, then any reference to
 * meta($$ds) will be rewritten as $$7.
 *
 * meta($$ds) means "get the meta record of the data source ds".
 * </pre>
 */
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

        boolean changed = false;
        // recursively transform argument expressions.
        for (Mutable<ILogicalExpression> argRef : argRefs) {
            changed |= transform(argRef);
        }

        if (!funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.META)) {
            return changed;
        }
        // the user query provides more than one parameter for the meta function.
        if (argRefs.size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                    "The meta function can at most have one argument!");
        }

        // the user query provides exact one parameter for the meta function.
        if (argRefs.size() == 1) {
            ILogicalExpression argExpr = argRefs.get(0).getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return changed;
            }
            VariableReferenceExpression argVarExpr = (VariableReferenceExpression) argExpr;
            LogicalVariable argVar = argVarExpr.getVariableReference();
            if (!dataVar.equals(argVar)) {
                return changed;
            }
            VariableReferenceExpression metaVarRef = new VariableReferenceExpression(metaVar);
            metaVarRef.setSourceLocation(expr.getSourceLocation());
            exprRef.setValue(metaVarRef);
            return true;
        }

        // the user query provides zero parameter for the meta function.
        if (variableRequired) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                    "Cannot resolve ambiguous meta function call. There are more than one dataset choice!");
        }
        VariableReferenceExpression metaVarRef = new VariableReferenceExpression(metaVar);
        metaVarRef.setSourceLocation(expr.getSourceLocation());
        exprRef.setValue(metaVarRef);
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
        // tries transformations one by one.
        for (ILogicalExpressionReferenceTransform transformer : transformers) {
            if (transformer.transform(expression)) {
                return true;
            }
        }
        return false;
    }
}

/**
 * <pre>
 * This class replaces meta-key() references with their corresponding field accessors. It maintains the meta
 * variable that will replace the meta-key(). Meta-key() acts as a field access of the meta record. For example:
 * If the meta record variable is $$7, meta-key($$ds, "address.zip") will be rewritten as $$7.address.zip.
 *
 * meta-key($$ds, "address.zip") means "access the field address.zip of the meta record of data source ds".
 * </pre>
 */
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
        if (!funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.META_KEY)) {
            return false;
        }
        SourceLocation sourceLoc = expr.getSourceLocation();
        // get arguments. First argument : Resource key, second argument: field
        // TODO: how come arg 1 (the data source) is not checked?
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        ConstantExpression fieldNameExpression = (ConstantExpression) args.get(1).getValue();
        AsterixConstantValue fieldNameValue = (AsterixConstantValue) fieldNameExpression.getValue();
        IAType fieldNameType = fieldNameValue.getObject().getType();
        FunctionIdentifier functionIdentifier;
        switch (fieldNameType.getTypeTag()) {
            case ARRAY:
                // field access nested
                functionIdentifier = BuiltinFunctions.FIELD_ACCESS_NESTED;
                break;
            case STRING:
                // field access by name
                functionIdentifier = BuiltinFunctions.FIELD_ACCESS_BY_NAME;
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Unsupported field name type " + fieldNameType.getTypeTag());
        }
        IFunctionInfo finfoAccess = FunctionUtil.getFunctionInfo(functionIdentifier);
        ArrayList<Mutable<ILogicalExpression>> argExprs = new ArrayList<>(2);
        VariableReferenceExpression metaVarRef = new VariableReferenceExpression(metaVar);
        metaVarRef.setSourceLocation(sourceLoc);
        argExprs.add(new MutableObject<>(metaVarRef));
        argExprs.add(new MutableObject<>(fieldNameExpression));
        ScalarFunctionCallExpression fAccessExpr = new ScalarFunctionCallExpression(finfoAccess, argExprs);
        fAccessExpr.setSourceLocation(sourceLoc);
        exprRef.setValue(fAccessExpr);
        return true;
    }
}

/**
 * <pre>
 * This class replaces meta-key() references with their corresponding logical variables. It maintains a list of
 * meta-key() references together with their logical variables (the logical variables being the primary key variables
 * of the data source). For example:
 * primary key variable (i.e. keyVars): $$1, $$2
 * meta-key() references (i.e. metaKeyAccessExpressions): meta-key($$ds, "id1"), meta-key($$ds, "id2")
 *
 * Any reference to meta-key($$ds, "id1") will be rewritten as $$1.
 *
 * meta-key($$ds, "id1") means "access the field id1 of the meta record of data source ds which is also a primary key".
 *
 * "id1" and "id2" are the primary keys of the data source "ds". They are fields of the meta record (not $$ds record).
 * </pre>
 */
class MetaKeyExpressionReferenceTransform implements ILogicalExpressionReferenceTransformWithCondition {
    private final List<LogicalVariable> keyVars;
    private final List<ScalarFunctionCallExpression> metaKeyAccessExpressions;

    MetaKeyExpressionReferenceTransform(List<LogicalVariable> keyVars,
            List<ScalarFunctionCallExpression> metaKeyAccessExpressions) {
        this.keyVars = keyVars;
        this.metaKeyAccessExpressions = metaKeyAccessExpressions;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> exprRef) {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.META_KEY)) {
            return false;
        }

        // function is meta key access
        for (int i = 0; i < metaKeyAccessExpressions.size(); i++) {
            if (metaKeyAccessExpressions.get(i).equals(funcExpr)) {
                VariableReferenceExpression varRef = new VariableReferenceExpression(keyVars.get(i));
                varRef.setSourceLocation(expr.getSourceLocation());
                exprRef.setValue(varRef);
                return true;
            }
        }
        return false;
    }
}
