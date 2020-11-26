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

package org.apache.asterix.lang.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.parser.FunctionParser;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class FunctionUtil {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";

    private static final DataverseName FN_DATASET_DATAVERSE_NAME =
            FunctionSignature.getDataverseName(BuiltinFunctions.DATASET);

    private static final String FN_DATASET_NAME = BuiltinFunctions.DATASET.getName();

    /**
     * @deprecated use {@link BuiltinFunctions#getBuiltinFunctionInfo(FunctionIdentifier)} instead
     */
    public static BuiltinFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return BuiltinFunctions.getBuiltinFunctionInfo(fi);
    }

    public static TypeSignature getTypeDependencyFromFunctionParameter(TypeExpression typeExpr,
            DataverseName defaultDataverse) {
        switch (typeExpr.getTypeKind()) {
            case ORDEREDLIST:
                return getTypeDependencyFromFunctionParameter(
                        ((OrderedListTypeDefinition) typeExpr).getItemTypeExpression(), defaultDataverse);
            case UNORDEREDLIST:
                return getTypeDependencyFromFunctionParameter(
                        ((UnorderedListTypeDefinition) typeExpr).getItemTypeExpression(), defaultDataverse);
            case TYPEREFERENCE:
                TypeReferenceExpression typeRef = ((TypeReferenceExpression) typeExpr);
                String typeName = typeRef.getIdent().getSecond().toString();
                BuiltinType builtinType = BuiltinTypeMap.getBuiltinType(typeName);
                if (builtinType != null) {
                    return null;
                }
                DataverseName typeDataverseName =
                        typeRef.getIdent().getFirst() != null ? typeRef.getIdent().getFirst() : defaultDataverse;
                return new TypeSignature(typeDataverseName, typeName);
            case RECORD:
                throw new IllegalArgumentException();
            default:
                throw new IllegalStateException();
        }
    }

    @FunctionalInterface
    public interface IFunctionCollector {
        Set<CallExpr> getFunctionCalls(Expression expression) throws CompilationException;
    }

    public static FunctionSignature resolveFunctionCall(FunctionSignature fs, SourceLocation sourceLoc,
            MetadataProvider metadataProvider, Set<FunctionSignature> declaredFunctions,
            BiFunction<String, Integer, FunctionSignature> builtinFunctionResolver) throws CompilationException {
        int arity = fs.getArity();
        DataverseName dataverse = fs.getDataverseName();
        if (dataverse == null) {
            dataverse = metadataProvider.getDefaultDataverseName();
        }
        boolean isBuiltinFuncDataverse =
                dataverse.equals(FunctionConstants.ASTERIX_DV) || dataverse.equals(FunctionConstants.ALGEBRICKS_DV);

        if (!isBuiltinFuncDataverse) {
            // attempt to resolve to a user-defined function
            FunctionSignature fsWithDv =
                    fs.getDataverseName() == null ? new FunctionSignature(dataverse, fs.getName(), arity) : fs;
            if (declaredFunctions.contains(fsWithDv)) {
                return fsWithDv;
            }
            FunctionSignature fsWithDvVarargs =
                    new FunctionSignature(fsWithDv.getDataverseName(), fsWithDv.getName(), FunctionIdentifier.VARARGS);
            if (declaredFunctions.contains(fsWithDvVarargs)) {
                return fsWithDvVarargs;
            }
            try {
                Function function = metadataProvider.lookupUserDefinedFunction(fsWithDv);
                if (function != null) {
                    return fsWithDv;
                }
                function = metadataProvider.lookupUserDefinedFunction(fsWithDvVarargs);
                if (function != null) {
                    return fsWithDvVarargs;
                }
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, sourceLoc, e.getMessage());
            }

            // fail if the dataverse was specified in the function call but this dataverse does not exist
            if (fs.getDataverseName() != null) {
                Dataverse dv;
                try {
                    dv = metadataProvider.findDataverse(dataverse);
                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, sourceLoc, e.getMessage());
                }
                if (dv == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, dataverse);
                }
            }
        }

        // attempt to resolve to a built-in function
        String name = fs.getName().toLowerCase();
        String mappedName = CommonFunctionMapUtil.getFunctionMapping(name);
        if (mappedName != null) {
            name = mappedName;
        }
        FunctionSignature fsBuiltin = builtinFunctionResolver.apply(name, arity);
        if (fsBuiltin == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, fs.toString(false));
        }
        return fsBuiltin;
    }

    public static BiFunction<String, Integer, FunctionSignature> createBuiltinFunctionResolver(
            MetadataProvider metadataProvider) {
        boolean includePrivateFunctions = getImportPrivateFunctions(metadataProvider);
        return createBuiltinFunctionResolver(includePrivateFunctions);
    }

    public static BiFunction<String, Integer, FunctionSignature> createBuiltinFunctionResolver(
            boolean includePrivateFunctions) {
        return (name, arity) -> {
            String builtinName = name.replace('_', '-');
            BuiltinFunctionInfo finfo = BuiltinFunctions.resolveBuiltinFunction(builtinName, arity);
            if (finfo == null) {
                return null;
            }
            if (!includePrivateFunctions && finfo.isPrivate()) {
                return null;
            }
            return new FunctionSignature(finfo.getFunctionIdentifier());
        };
    }

    /**
     * Retrieve stored functions (from CREATE FUNCTION statements) that have been
     * used in an expression.
     *
     * @param metadataProvider,
     *            the metadata provider
     * @param expression,
     *            the expression for analysis
     * @param declaredFunctions,
     *            a set of declared functions in the query, which can potentially
     *            override stored functions.
     * @param functionCollector,
     *            for collecting function calls in the <code>expression</code>
     * @param functionParser,
     *            for parsing stored functions in the string represetnation.
     * @param defaultDataverse
     * @throws CompilationException
     */
    public static List<FunctionDecl> retrieveUsedStoredFunctions(MetadataProvider metadataProvider,
            Expression expression, List<FunctionSignature> declaredFunctions, List<FunctionDecl> inputFunctionDecls,
            IFunctionCollector functionCollector, FunctionParser functionParser, DataverseName defaultDataverse)
            throws CompilationException {
        if (expression == null) {
            return Collections.emptyList();
        }
        List<FunctionDecl> functionDecls =
                inputFunctionDecls == null ? new ArrayList<>() : new ArrayList<>(inputFunctionDecls);
        Set<CallExpr> functionCalls = functionCollector.getFunctionCalls(expression);
        Set<FunctionSignature> functionSignatures = new HashSet<>();
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature fs = functionCall.getFunctionSignature();
            if (fs.getDataverseName() == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, functionCall.getSourceLocation(),
                        fs);
            }
            if (!functionSignatures.add(fs)) {
                // already seen this signature
                continue;
            }
            if (declaredFunctions != null && declaredFunctions.contains(fs)) {
                continue;
            }
            Function function;
            try {
                function = metadataProvider.lookupUserDefinedFunction(fs);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, functionCall.getSourceLocation(),
                        e.toString());
            }
            if (function == null || !functionParser.getLanguage().equals(function.getLanguage())) {
                // the function is either unknown, builtin, or in a different language.
                // either way we ignore it here because it will be handled by the function inlining rule later
                continue;
            }

            FunctionDecl functionDecl = functionParser.getFunctionDecl(function);
            if (functionDecls.contains(functionDecl)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, functionCall.getSourceLocation(),
                        "Recursive invocation " + functionDecls.get(functionDecls.size() - 1).getSignature() + " <==> "
                                + functionDecl.getSignature());
            }
            functionDecls.add(functionDecl);
            functionDecls = retrieveUsedStoredFunctions(metadataProvider, functionDecl.getFuncBody(), declaredFunctions,
                    functionDecls, functionCollector, functionParser, function.getDataverseName());
        }
        return functionDecls;
    }

    public static List<List<Triple<DataverseName, String, String>>> getFunctionDependencies(IQueryRewriter rewriter,
            Expression expression, MetadataProvider metadataProvider) throws CompilationException {
        Set<CallExpr> functionCalls = rewriter.getFunctionCalls(expression);
        //Get the List of used functions and used datasets
        List<Triple<DataverseName, String, String>> datasourceDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature signature = functionCall.getFunctionSignature();
            if (isBuiltinDatasetFunction(signature)) {
                Pair<DataverseName, String> datasetReference = parseDatasetFunctionArguments(functionCall);
                datasourceDependencies.add(new Triple<>(datasetReference.first, datasetReference.second, null));
            } else if (BuiltinFunctions.getBuiltinFunctionInfo(signature.createFunctionIdentifier()) == null) {
                functionDependencies.add(new Triple<>(signature.getDataverseName(), signature.getName(),
                        Integer.toString(signature.getArity())));
            }
        }
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>(3);
        dependencies.add(datasourceDependencies);
        dependencies.add(functionDependencies);
        dependencies.add(Collections.emptyList());
        return dependencies;
    }

    public static List<List<Triple<DataverseName, String, String>>> getExternalFunctionDependencies(
            Collection<TypeSignature> dependentTypes) {
        List<Triple<DataverseName, String, String>> datasourceDependencies = Collections.emptyList();
        List<Triple<DataverseName, String, String>> functionDependencies = Collections.emptyList();
        List<Triple<DataverseName, String, String>> typeDependencies = new ArrayList<>(dependentTypes.size());
        for (TypeSignature t : dependentTypes) {
            typeDependencies.add(new Triple<>(t.getDataverseName(), t.getName(), null));
        }
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>(3);
        dependencies.add(datasourceDependencies);
        dependencies.add(functionDependencies);
        dependencies.add(typeDependencies);
        return dependencies;
    }

    public static boolean isBuiltinDatasetFunction(FunctionSignature fs) {
        return Objects.equals(FN_DATASET_DATAVERSE_NAME, fs.getDataverseName())
                && Objects.equals(FN_DATASET_NAME, fs.getName());
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(CallExpr datasetFn)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn.getExprList(), datasetFn.getSourceLocation(),
                ExpressionUtils::getStringLiteral);
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(AbstractFunctionCallExpression datasetFn)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn.getArguments(), datasetFn.getSourceLocation(),
                FunctionUtil::getStringConstant);
    }

    private static <T> Pair<DataverseName, String> parseDatasetFunctionArguments(List<T> datasetFnArgs,
            SourceLocation sourceLoc, java.util.function.Function<T, String> argExtractFunction)
            throws CompilationException {
        if (datasetFnArgs.size() != 2) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Invalid number of arguments to dataset()");
        }
        String dataverseNameArg = argExtractFunction.apply(datasetFnArgs.get(0));
        if (dataverseNameArg == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseNameArg);

        String datasetName = argExtractFunction.apply(datasetFnArgs.get(1));
        if (datasetName == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        return new Pair<>(dataverseName, datasetName);
    }

    private static String getStringConstant(Mutable<ILogicalExpression> arg) {
        return ConstantExpressionUtil.getStringConstant(arg.getValue());
    }

    private static boolean getImportPrivateFunctions(MetadataProvider metadataProvider) {
        String value = (String) metadataProvider.getConfig().get(IMPORT_PRIVATE_FUNCTIONS);
        return (value != null) && Boolean.parseBoolean(value.toLowerCase());
    }

    public static Set<FunctionSignature> getFunctionSignatures(List<FunctionDecl> declaredFunctions) {
        if (declaredFunctions == null || declaredFunctions.isEmpty()) {
            return Collections.emptySet();
        }
        Set<FunctionSignature> result = new HashSet<>();
        for (FunctionDecl fd : declaredFunctions) {
            result.add(fd.getSignature());
        }
        return result;
    }
}
