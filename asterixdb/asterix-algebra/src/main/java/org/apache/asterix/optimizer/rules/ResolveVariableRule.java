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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule resolves references to undefined identifiers as:
 * 1. expression + field-access paths, or
 * 2. datasets
 * based on the available type and metadata information.
 */
public class ResolveVariableRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getInputs().isEmpty()) {
            return false;
        }
        // Populates the latest type information, e.g., resolved path sugars.
        context.computeAndSetTypeEnvironmentForOperator(op);
        if (op.acceptExpressionTransform(
                exprRef -> rewriteExpressionReference(op, exprRef, new Triple<>(false, null, null), null, context))) {
            // Generates the up-to-date type information.
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    // Recursively rewrites for an expression within an operator.
    private boolean rewriteExpressionReference(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            Triple<Boolean, String, String> fullyQualifiedDatasetPathCandidateFromParent,
            Mutable<ILogicalExpression> parentFuncRef, IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean changed = false;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        Triple<Boolean, String, String> fullyQualifiedDatasetPathCandidate = resolveFullyQualifiedPath(funcExpr,
                context);
        for (Mutable<ILogicalExpression> funcArgRef : funcExpr.getArguments()) {
            if (rewriteExpressionReference(op, funcArgRef, fullyQualifiedDatasetPathCandidate, exprRef, context)) {
                changed = true;
            }
        }

        // Cleans up extra scan-collections if there is.
        if (changed) {
            cleanupScanCollectionForDataset(funcExpr);
        }

        // Does the actual resolution.
        return changed || resolve(op, context, exprRef, fullyQualifiedDatasetPathCandidateFromParent, parentFuncRef);
    }

    // Resolves a "resolve" function call expression to a fully qualified variable/field-access path or
    // a dataset.
    private boolean resolve(ILogicalOperator op, IOptimizationContext context, Mutable<ILogicalExpression> exprRef,
            Triple<Boolean, String, String> fullyQualifiedDatasetPathCandidateFromParent,
            Mutable<ILogicalExpression> parentFuncRef) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exprRef.getValue();
        if (funcExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.RESOLVE) {
            return false;
        }
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        String unresolvedVarName = extractConstantString(arg);
        return resolveInternal(exprRef, hasMatchedDatasetForVariableName(unresolvedVarName, context),
                findCandidatePaths(op, extractExprs(funcExpr.getArguments()), unresolvedVarName, context),
                unresolvedVarName, fullyQualifiedDatasetPathCandidateFromParent, parentFuncRef, context);
    }

    // Extracts all possible expressions from the arguments of the "resolve" function.
    private List<ILogicalExpression> extractExprs(List<Mutable<ILogicalExpression>> args) throws AlgebricksException {
        List<ILogicalExpression> exprs = new ArrayList<>();
        // The first arg is is the name of the undefined variable.
        for (int index = 1; index < args.size(); ++index) {
            ILogicalExpression argExpr = args.get(index).getValue();
            exprs.add(argExpr);
        }
        return exprs;
    }

    // Resolves an undefined name to a dataset or a fully qualified variable/field-access path
    // based on the given information of dataset matches and candidate paths.
    private boolean resolveInternal(Mutable<ILogicalExpression> funcRef, boolean hasMatchedDataset,
            Collection<Pair<ILogicalExpression, List<String>>> varAccessCandidates, String unresolvedVarName,
            Triple<Boolean, String, String> fullyQualifiedDatasetPathCandidateFromParent,
            Mutable<ILogicalExpression> parentFuncRef, IOptimizationContext context) throws AlgebricksException {
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) funcRef.getValue();
        int numVarCandidates = varAccessCandidates.size();
        boolean hasAmbiguity = hasAmbiguity(hasMatchedDataset, fullyQualifiedDatasetPathCandidateFromParent,
                numVarCandidates);
        if (hasAmbiguity) {
            // More than one possibilities.
            throw new AlgebricksException("Cannot resolve ambiguous alias reference for undefined identifier "
                    + unresolvedVarName);
        } else if (hasMatchedDataset) {
            // Rewrites the "resolve" function to a "dataset" function and only keep the dataset name argument.
            func.setFunctionInfo(FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.DATASET));
            Mutable<ILogicalExpression> datasetNameExpression = func.getArguments().get(0);
            func.getArguments().clear();
            func.getArguments().add(datasetNameExpression);
        } else if (fullyQualifiedDatasetPathCandidateFromParent.first) {
            // Rewrites the parent "field-access" function to a "dataset" function.
            AbstractFunctionCallExpression parentFunc = (AbstractFunctionCallExpression) parentFuncRef.getValue();
            parentFunc.setFunctionInfo(FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.DATASET));
            parentFunc.getArguments().clear();
            parentFunc.getArguments().add(
                    new MutableObject<>(new ConstantExpression(
                            new AsterixConstantValue(new AString(fullyQualifiedDatasetPathCandidateFromParent.second
                                    + "." + fullyQualifiedDatasetPathCandidateFromParent.third)))));
        } else if (numVarCandidates == 1) {
            resolveAsFieldAccess(funcRef, varAccessCandidates.iterator().next());
        } else {
            MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
            // Cannot find any resolution.
            throw new AlgebricksException("Cannot find dataset " + unresolvedVarName + " in dataverse "
                    + metadataProvider.getDefaultDataverseName() + " nor an alias with name " + unresolvedVarName);
        }
        return true;
    }

    // Check whether it is possible to have multiple resolutions for a "resolve" function.
    private boolean hasAmbiguity(boolean hasMatchedDataset,
            Triple<Boolean, String, String> fullyQualifiedDatasetPathCandidateFromParent, int numVarCandidates) {
        boolean hasAmbiguity = numVarCandidates > 1 || (numVarCandidates == 1 && hasMatchedDataset);
        hasAmbiguity = hasAmbiguity || (numVarCandidates == 1 && fullyQualifiedDatasetPathCandidateFromParent.first);
        hasAmbiguity = hasAmbiguity || (hasMatchedDataset && fullyQualifiedDatasetPathCandidateFromParent.first);
        return hasAmbiguity;
    }

    // Resolves a "resolve" function call as a field access.
    private void resolveAsFieldAccess(Mutable<ILogicalExpression> funcRef,
            Pair<ILogicalExpression, List<String>> varAndPath) {
        // Rewrites to field-access-by-names.
        ILogicalExpression expr = varAndPath.first;
        List<String> path = varAndPath.second;
        Mutable<ILogicalExpression> firstArgRef = new MutableObject<>(expr);
        ILogicalExpression newFunc = null;
        for (String fieldName : path) {
            List<Mutable<ILogicalExpression>> args = new ArrayList<>();
            args.add(firstArgRef);
            args.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString(fieldName)))));
            newFunc = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME), args);
            firstArgRef = new MutableObject<>(newFunc);
        }
        funcRef.setValue(newFunc);
    }

    // Finds all candidate fully qualified expression/field-access paths.
    private Set<Pair<ILogicalExpression, List<String>>> findCandidatePaths(ILogicalOperator op,
            Collection<ILogicalExpression> referenceExprs, String unresolvedVarName, IOptimizationContext context)
            throws AlgebricksException {
        Set<Pair<ILogicalExpression, List<String>>> candidates = new HashSet<>();
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op.getInputs().get(0).getValue());
        for (ILogicalExpression referenceExpr : referenceExprs) {
            IAType type = (IAType) env.getType(referenceExpr);
            candidates.addAll(findCandidatePathsForExpr(unresolvedVarName, type, referenceExpr, new ArrayList<>()));
        }
        return candidates;
    }

    // Recursively finds candidate paths under an expression.
    private Set<Pair<ILogicalExpression, List<String>>> findCandidatePathsForExpr(String unresolvedVarName,
            IAType pathType, ILogicalExpression expr, List<String> parentPath) throws AlgebricksException {
        Set<Pair<ILogicalExpression, List<String>>> varAccessCandidates = new HashSet<>();
        IAType type = pathType;
        if (type.getTypeTag() == ATypeTag.UNION) {
            type = ((AUnionType) type).getActualType();
        }
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            List<String> path = new ArrayList<>(parentPath);
            path.add(unresolvedVarName);
            varAccessCandidates.add(new Pair<>(expr, path));
        }
        if (tag == ATypeTag.RECORD) {
            ARecordType recordType = (ARecordType) type;
            if (recordType.canContainField(unresolvedVarName)) {
                // If the field name is possible.
                List<String> path = new ArrayList<>(parentPath);
                path.add(unresolvedVarName);
                varAccessCandidates.add(new Pair<>(expr, path));
            } else {
                // Recursively identified possible paths.
                String[] fieldNames = recordType.getFieldNames();
                IAType[] fieldTypes = recordType.getFieldTypes();
                for (int index = 0; index < fieldNames.length; ++index) {
                    List<String> path = new ArrayList<>(parentPath);
                    path.add(fieldNames[index]);
                    varAccessCandidates.addAll(findCandidatePathsForExpr(unresolvedVarName, fieldTypes[index], expr,
                            path));
                }
            }
        }
        return varAccessCandidates;
    }

    // Try to resolve the expression like resolve("x").foo as x.foo.
    private Triple<Boolean, String, String> resolveFullyQualifiedPath(AbstractFunctionCallExpression funcExpr,
            IOptimizationContext context) throws AlgebricksException {
        if (!funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            return new Triple<>(false, null, null);
        }
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        ILogicalExpression firstExpr = args.get(0).getValue();
        ILogicalExpression secondExpr = args.get(1).getValue();
        if (firstExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return new Triple<>(false, null, null);
        }
        if (secondExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return new Triple<>(false, null, null);
        }
        AbstractFunctionCallExpression firstFuncExpr = (AbstractFunctionCallExpression) firstExpr;
        if (!firstFuncExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.RESOLVE)) {
            return new Triple<>(false, null, null);
        }
        ILogicalExpression dataverseNameExpr = firstFuncExpr.getArguments().get(0).getValue();
        String dataverseName = extractConstantString(dataverseNameExpr);
        String datasetName = extractConstantString(secondExpr);
        return new Triple<>(hasMatchedDataverseDataset(dataverseName, datasetName, context), dataverseName,
                datasetName);
    }

    // Checks whether the dataverse name and dataset name matche a dataset.
    private boolean hasMatchedDataverseDataset(String dataverseName, String datasetName, IOptimizationContext context)
            throws AlgebricksException {
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        return mdp.findDataset(dataverseName, datasetName) != null;
    }

    // Checks whether the name matches a dataset.
    private boolean hasMatchedDatasetForVariableName(String varName, IOptimizationContext context)
            throws AlgebricksException {
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        if (mdp.findDataset(mdp.getDefaultDataverseName(), varName) != null) {
            return true;
        }
        if (varName.contains(".")) {
            String[] path = varName.split("\\.");
            if (path.length != 2) {
                return false;
            }
            if (mdp.findDataset(path[0], path[1]) != null) {
                return true;
            }
        }
        return false;
    }

    // Cleans up scan collections on top of a "dataset" function call since "dataset"
    // is an unnest function.
    private void cleanupScanCollectionForDataset(AbstractFunctionCallExpression funcExpr) {
        if (funcExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SCAN_COLLECTION) {
            return;
        }
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) arg;
        if (argFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.DATASET) {
            return;
        }
        funcExpr.setFunctionInfo(argFuncExpr.getFunctionInfo());
        funcExpr.getArguments().clear();
        funcExpr.getArguments().addAll(argFuncExpr.getArguments());
    }

    // Extracts the name of an undefined variable.
    private String extractConstantString(ILogicalExpression arg) throws AlgebricksException {
        final String str = ConstantExpressionUtil.getStringConstant(arg);
        if (str == null) {
            throw new AlgebricksException("The argument is expected to be a string constant value.");
        }
        return str;
    }
}
