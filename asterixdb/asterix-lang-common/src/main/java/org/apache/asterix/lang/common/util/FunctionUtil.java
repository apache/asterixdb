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

import static org.apache.asterix.common.functions.FunctionConstants.ASTERIX_DV;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.parser.FunctionParser;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class FunctionUtil {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";

    private static final DataverseName FN_DATASET_DATAVERSE_NAME =
            FunctionSignature.getDataverseName(BuiltinFunctions.DATASET);

    private static final String FN_DATASET_NAME = BuiltinFunctions.DATASET.getName();

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return BuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(FunctionSignature fs) {
        return getFunctionInfo(fs.createFunctionIdentifier());
    }

    public static IFunctionInfo getBuiltinFunctionInfo(String functionName, int arity) {
        IFunctionInfo fi =
                getFunctionInfo(new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, functionName, arity));
        if (fi == null) {
            fi = getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS, functionName, arity));
        }
        return fi;
    }

    public static Pair<DataverseName, String> getDependencyFromParameterType(IndexedTypeExpression parameterType,
            DataverseName defaultDataverse) {
        Pair<DataverseName, String> typeName =
                FunctionUtil.extractNestedTypeName(parameterType.getType(), defaultDataverse);
        return typeName == null || ASTERIX_DV.equals(typeName.getFirst()) ? null : typeName;
    }

    private static Pair<DataverseName, String> extractNestedTypeName(TypeExpression typeExpr,
            DataverseName defaultDataverse) {
        switch (typeExpr.getTypeKind()) {
            case ORDEREDLIST:
                return extractNestedTypeName(((OrderedListTypeDefinition) typeExpr).getItemTypeExpression(),
                        defaultDataverse);
            case UNORDEREDLIST:
                return extractNestedTypeName(((UnorderedListTypeDefinition) typeExpr).getItemTypeExpression(),
                        defaultDataverse);
            case RECORD:
                break;
            case TYPEREFERENCE:
                TypeReferenceExpression typeRef = ((TypeReferenceExpression) typeExpr);
                String typeName = typeRef.getIdent().getSecond().toString();
                DataverseName typeDv = BuiltinTypeMap.getBuiltinType(typeName) != null ? ASTERIX_DV
                        : typeRef.getIdent().getFirst() != null ? typeRef.getIdent().getFirst() : defaultDataverse;
                return new Pair<>(typeDv, typeName);
        }
        return null;
    }

    @FunctionalInterface
    public interface IFunctionCollector {
        Set<CallExpr> getFunctionCalls(Expression expression) throws CompilationException;
    }

    @FunctionalInterface
    public interface IFunctionNormalizer {
        FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs, SourceLocation sourceLoc)
                throws CompilationException;
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
     * @param functionNormalizer,
     *            for normalizing function names.
     * @throws CompilationException
     */
    public static List<FunctionDecl> retrieveUsedStoredFunctions(MetadataProvider metadataProvider,
            Expression expression, List<FunctionSignature> declaredFunctions, List<FunctionDecl> inputFunctionDecls,
            IFunctionCollector functionCollector, FunctionParser functionParser, IFunctionNormalizer functionNormalizer)
            throws CompilationException {
        List<FunctionDecl> functionDecls =
                inputFunctionDecls == null ? new ArrayList<>() : new ArrayList<>(inputFunctionDecls);
        if (expression == null) {
            return functionDecls;
        }
        String value = (String) metadataProvider.getConfig().get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS);
        boolean includePrivateFunctions = (value != null) && Boolean.parseBoolean(value.toLowerCase());
        Set<CallExpr> functionCalls = functionCollector.getFunctionCalls(expression);
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature signature = functionCall.getFunctionSignature();
            if (declaredFunctions != null && declaredFunctions.contains(signature)) {
                continue;
            }
            if (signature.getDataverseName() == null) {
                signature.setDataverseName(metadataProvider.getDefaultDataverseName());
            }
            DataverseName namespace = signature.getDataverseName();
            // Checks the existence of the referred dataverse.
            try {
                if (!namespace.equals(FunctionConstants.ASTERIX_DV)
                        && !namespace.equals(FunctionConstants.ALGEBRICKS_DV)
                        && metadataProvider.findDataverse(namespace) == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, functionCall.getSourceLocation(),
                            "In function call \"" + namespace + "." + signature.getName() + "(...)\", the dataverse \""
                                    + namespace + "\" cannot be found!");
                }
            } catch (AlgebricksException e) {
                throw new CompilationException(e);
            }
            Function function;
            try {
                function = lookupUserDefinedFunctionDecl(metadataProvider.getMetadataTxnContext(), signature);
            } catch (AlgebricksException e) {
                throw new CompilationException(e);
            }
            if (function == null) {
                FunctionSignature normalizedSignature = functionNormalizer == null ? signature
                        : functionNormalizer.normalizeBuiltinFunctionSignature(signature,
                                functionCall.getSourceLocation());
                if (BuiltinFunctions.isBuiltinCompilerFunction(normalizedSignature, includePrivateFunctions)) {
                    continue;
                }
                StringBuilder messageBuilder = new StringBuilder();
                if (!functionDecls.isEmpty()) {
                    messageBuilder.append("function " + functionDecls.get(functionDecls.size() - 1).getSignature()
                            + " depends upon function " + signature + " which is undefined");
                } else {
                    messageBuilder.append("function " + signature + " is not defined");
                }
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, functionCall.getSourceLocation(),
                        messageBuilder.toString());
            }

            if (functionParser.getLanguage().equals(function.getLanguage())) {
                FunctionDecl functionDecl = functionParser.getFunctionDecl(function);
                if (functionDecl != null) {
                    if (functionDecls.contains(functionDecl)) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, functionCall.getSourceLocation(),
                                "Recursive invocation " + functionDecls.get(functionDecls.size() - 1).getSignature()
                                        + " <==> " + functionDecl.getSignature());
                    }
                    functionDecls.add(functionDecl);
                    functionDecls = retrieveUsedStoredFunctions(metadataProvider, functionDecl.getFuncBody(),
                            declaredFunctions, functionDecls, functionCollector, functionParser, functionNormalizer);
                }
            }
        }
        return functionDecls;
    }

    public static List<List<Triple<DataverseName, String, String>>> getFunctionDependencies(IQueryRewriter rewriter,
            Expression expression, MetadataProvider metadataProvider, Collection<Pair<DataverseName, String>> argTypes)
            throws CompilationException {
        Set<CallExpr> functionCalls = rewriter.getFunctionCalls(expression);
        //Get the List of used functions and used datasets
        List<Triple<DataverseName, String, String>> datasourceDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> typeDependencies = new ArrayList<>();
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature signature = functionCall.getFunctionSignature();
            if (isBuiltinDatasetFunction(signature)) {
                Pair<DataverseName, String> datasetReference = parseDatasetFunctionArguments(functionCall.getExprList(),
                        metadataProvider.getDefaultDataverseName(), functionCall.getSourceLocation(),
                        ExpressionUtils::getStringLiteral);
                datasourceDependencies.add(new Triple<>(datasetReference.first, datasetReference.second, null));
            } else if (!BuiltinFunctions.isBuiltinCompilerFunction(signature, false)) {
                functionDependencies.add(new Triple<>(signature.getDataverseName(), signature.getName(),
                        Integer.toString(signature.getArity())));
            }
        }
        for (Pair<DataverseName, String> t : argTypes) {
            typeDependencies.add(new Triple<>(t.getFirst(), t.getSecond(), null));
        }
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>(3);
        dependencies.add(datasourceDependencies);
        dependencies.add(functionDependencies);
        dependencies.add(typeDependencies);
        return dependencies;
    }

    public static List<List<Triple<DataverseName, String, String>>> getExternalFunctionDependencies(
            Collection<Pair<DataverseName, String>> argTypes) {
        List<Triple<DataverseName, String, String>> datasourceDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> typeDependencies = new ArrayList<>();
        for (Pair<DataverseName, String> t : argTypes) {
            typeDependencies.add(new Triple<>(t.getFirst(), t.getSecond(), null));
        }
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>(3);
        dependencies.add(datasourceDependencies);
        dependencies.add(functionDependencies);
        dependencies.add(typeDependencies);
        return dependencies;
    }

    public static Function lookupUserDefinedFunctionDecl(MetadataTransactionContext mdTxnCtx,
            FunctionSignature signature) throws AlgebricksException {
        if (signature.getDataverseName() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

    public static boolean isBuiltinDatasetFunction(FunctionSignature fs) {
        return Objects.equals(FN_DATASET_DATAVERSE_NAME, fs.getDataverseName())
                && Objects.equals(FN_DATASET_NAME, fs.getName());
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(
            List<Mutable<ILogicalExpression>> datasetFnArgs, DataverseName defaultDataverseName,
            SourceLocation sourceLoc) throws CompilationException {
        return parseDatasetFunctionArguments(datasetFnArgs, defaultDataverseName, sourceLoc,
                FunctionUtil::getStringConstant);
    }

    public static <T> Pair<DataverseName, String> parseDatasetFunctionArguments(List<T> datasetFnArgs,
            DataverseName defaultDataverseName, SourceLocation sourceLoc,
            java.util.function.Function<T, String> argExtractFunction) throws CompilationException {
        DataverseName dataverseName;
        String datasetName;
        switch (datasetFnArgs.size()) {
            case 1: // AQL BACK-COMPAT case
                String datasetArgBackCompat = argExtractFunction.apply(datasetFnArgs.get(0));
                if (datasetArgBackCompat == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Invalid argument to dataset()");
                }
                int pos = datasetArgBackCompat.indexOf('.');
                if (pos > 0 && pos < datasetArgBackCompat.length() - 1) {
                    dataverseName = DataverseName.createSinglePartName(datasetArgBackCompat.substring(0, pos)); // AQL BACK-COMPAT
                    datasetName = datasetArgBackCompat.substring(pos + 1);
                } else {
                    dataverseName = defaultDataverseName;
                    datasetName = datasetArgBackCompat;
                }
                break;
            case 2:
                String dataverseNameArg = argExtractFunction.apply(datasetFnArgs.get(0));
                if (dataverseNameArg == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Invalid argument to dataset()");
                }
                dataverseName = DataverseName.createFromCanonicalForm(dataverseNameArg);

                datasetName = argExtractFunction.apply(datasetFnArgs.get(1));
                if (datasetName == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Invalid argument to dataset()");
                }
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Invalid number of arguments to dataset()");
        }
        return new Pair<>(dataverseName, datasetName);
    }

    private static String getStringConstant(Mutable<ILogicalExpression> arg) {
        return ConstantExpressionUtil.getStringConstant(arg.getValue());
    }
}
