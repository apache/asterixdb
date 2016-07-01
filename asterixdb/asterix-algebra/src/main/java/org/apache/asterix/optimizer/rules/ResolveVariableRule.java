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
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule resolves references to undefined identifiers as:
 * 1. variable/field-access paths, or
 * 2. datasets
 * based on the available type and metatadata information.
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
        if (op.acceptExpressionTransform(exprRef -> rewriteExpressionReference(op, exprRef, context))) {
            // Generates the up-to-date type information.
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    // Recursively rewrites for an expression within an operator.
    private boolean rewriteExpressionReference(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean changed = false;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        for (Mutable<ILogicalExpression> funcArgRef : funcExpr.getArguments()) {
            if (rewriteExpressionReference(op, funcArgRef, context)) {
                context.computeAndSetTypeEnvironmentForOperator(op);
                changed = true;
            }
        }

        // Cleans up extra scan-collections if there is.
        cleanupScanCollectionForDataset(funcExpr);

        // Does the actual resolution.
        return changed || resolve(op, context, exprRef);
    }

    // Resolves a "resolve" function call expression to a fully qualified variable/field-access path or
    // a dataset.
    private boolean resolve(ILogicalOperator op, IOptimizationContext context, Mutable<ILogicalExpression> exprRef)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exprRef.getValue();
        if (funcExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.RESOLVE) {
            return false;
        }
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        String unresolvedVarName = extractVariableName(arg);
        return resolveInternal(exprRef, hasMatchedDataset(unresolvedVarName, context),
                findCandidatePaths(op, extractPossibleVariables(funcExpr.getArguments()), unresolvedVarName, context),
                unresolvedVarName);
    }

    // Extracts all possible variables from the arguments of the "resolve" function.
    private List<LogicalVariable> extractPossibleVariables(List<Mutable<ILogicalExpression>> args)
            throws AlgebricksException {
        List<LogicalVariable> vars = new ArrayList<>();
        // The first arg is is the name of the undefined variable.
        for (int index = 1; index < args.size(); ++index) {
            vars.add(extractVariable(args.get(index).getValue()));
        }
        return vars;
    }

    // Resolves an undefined name to a dataset or a fully qualified variable/field-access path
    // based on the given information of dataset matches and candidate paths.
    private boolean resolveInternal(Mutable<ILogicalExpression> funcRef, boolean hasMatchedDataset,
            Collection<Pair<LogicalVariable, List<String>>> varAccessCandidates, String unresolvedVarName)
            throws AlgebricksException {
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) funcRef.getValue();
        int numVarCandidates = varAccessCandidates.size();
        if (numVarCandidates > 1 || (numVarCandidates == 1 && hasMatchedDataset)) {
            throw new AlgebricksException(
                    "Cannot resolve ambiguous alias (variable) reference for identifier " + unresolvedVarName);
        } else if (numVarCandidates <= 0) {
            if (!hasMatchedDataset) {
                throw new AlgebricksException(
                        "Undefined alias (variable) reference for identifier " + unresolvedVarName);
            }
            // Rewrites the "resolve" function to a "dataset" function.
            func.setFunctionInfo(FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.DATASET));
        } else {
            // Rewrites to field-access-by-names.
            Pair<LogicalVariable, List<String>> varAndPath = varAccessCandidates.iterator().next();
            LogicalVariable var = varAndPath.first;
            List<String> path = varAndPath.second;
            Mutable<ILogicalExpression> firstArgRef = new MutableObject<>(new VariableReferenceExpression(var));
            ILogicalExpression newFunc = null;
            for (String fieldName : path) {
                List<Mutable<ILogicalExpression>> args = new ArrayList<>();
                args.add(firstArgRef);
                args.add(new MutableObject<ILogicalExpression>(
                        new ConstantExpression(new AsterixConstantValue(new AString(fieldName)))));
                newFunc = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME), args);
                firstArgRef = new MutableObject<>(newFunc);
            }
            funcRef.setValue(newFunc);
        }
        return true;
    }

    // Finds all candidate fully qualified variable/field-access paths.
    private Set<Pair<LogicalVariable, List<String>>> findCandidatePaths(ILogicalOperator op,
            Collection<LogicalVariable> inputLiveVars, String unresolvedVarName, IOptimizationContext context)
            throws AlgebricksException {
        Set<Pair<LogicalVariable, List<String>>> candidates = new HashSet<>();
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op.getInputs().get(0).getValue());
        for (LogicalVariable var : inputLiveVars) {
            IAType type = (IAType) env.getVarType(var);
            candidates.addAll(findCandidatePathsForVariable(unresolvedVarName, type, var, new ArrayList<String>()));
        }
        return candidates;
    }

    // Recursively finds candidate paths under a variable.
    private Set<Pair<LogicalVariable, List<String>>> findCandidatePathsForVariable(String unresolvedVarName,
            IAType pathType, LogicalVariable var, List<String> parentPath) throws AlgebricksException {
        Set<Pair<LogicalVariable, List<String>>> varAccessCandidates = new HashSet<>();
        IAType type = pathType;
        if (type.getTypeTag() == ATypeTag.UNION) {
            type = ((AUnionType) type).getActualType();
        }
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            List<String> path = new ArrayList<>(parentPath);
            path.add(unresolvedVarName);
            varAccessCandidates.add(new Pair<>(var, path));
        }
        if (tag == ATypeTag.RECORD) {
            ARecordType recordType = (ARecordType) type;
            if (recordType.canContainField(unresolvedVarName)) {
                // If the field name is possible.
                List<String> path = new ArrayList<>(parentPath);
                path.add(unresolvedVarName);
                varAccessCandidates.add(new Pair<>(var, path));
            } else {
                // Recursively identified possible paths.
                String[] fieldNames = recordType.getFieldNames();
                IAType[] fieldTypes = recordType.getFieldTypes();
                for (int index = 0; index < fieldNames.length; ++index) {
                    List<String> path = new ArrayList<>(parentPath);
                    path.add(fieldNames[index]);
                    varAccessCandidates
                            .addAll(findCandidatePathsForVariable(unresolvedVarName, fieldTypes[index], var, path));
                }
            }
        }
        return varAccessCandidates;
    }

    // Checks whether the name matches a dataset.
    private boolean hasMatchedDataset(String name, IOptimizationContext context) throws AlgebricksException {
        AqlMetadataProvider mdp = (AqlMetadataProvider) context.getMetadataProvider();
        if (name.contains(".")) {
            String[] path = name.split("\\.");
            if (path.length != 2) {
                return false;
            }
            if (mdp.findDataset(path[0], path[1]) != null) {
                return true;
            }
        }
        if (mdp.findDataset(mdp.getDefaultDataverseName(), name) != null) {
            return true;
        }
        return false;
    }

    // Extracts the variable from a variable reference expression.
    private LogicalVariable extractVariable(ILogicalExpression expr) throws AlgebricksException {
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) expr;
            return varRefExpr.getVariableReference();
        } else {
            // The sugar visitor gurantees this would not happen.
            throw new AlgebricksException("The argument should be a variable reference expression.");
        }
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
    private String extractVariableName(ILogicalExpression arg) throws AlgebricksException {
        if (arg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            throw new AlgebricksException("The argument is expected to be a constant value.");
        }
        ConstantExpression ce = (ConstantExpression) arg;
        IAlgebricksConstantValue acv = ce.getValue();
        if (!(acv instanceof AsterixConstantValue)) {
            throw new AlgebricksException("The argument is expected to be an Asterix constant value.");
        }
        AsterixConstantValue acv2 = (AsterixConstantValue) acv;
        if (acv2.getObject().getType().getTypeTag() != ATypeTag.STRING) {
            throw new AlgebricksException("The argument is expected to be a string constant value.");
        }
        return ((AString) acv2.getObject()).getStringValue();
    }

}
