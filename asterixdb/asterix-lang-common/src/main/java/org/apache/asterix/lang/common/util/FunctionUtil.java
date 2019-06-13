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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class FunctionUtil {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return BuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(FunctionSignature fs) {
        return getFunctionInfo(new FunctionIdentifier(fs.getNamespace(), fs.getName(), fs.getArity()));
    }

    public static IFunctionInfo getBuiltinFunctionInfo(String functionName, int arity) {
        IFunctionInfo fi =
                getFunctionInfo(new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, functionName, arity));
        if (fi == null) {
            fi = getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS, functionName, arity));
        }
        return fi;
    }

    @FunctionalInterface
    public interface IFunctionCollector {
        Set<CallExpr> getFunctionCalls(Expression expression) throws CompilationException;
    }

    @FunctionalInterface
    public interface IFunctionParser {
        FunctionDecl getFunctionDecl(Function function) throws CompilationException;
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
            IFunctionCollector functionCollector, IFunctionParser functionParser,
            IFunctionNormalizer functionNormalizer) throws CompilationException {
        List<FunctionDecl> functionDecls =
                inputFunctionDecls == null ? new ArrayList<>() : new ArrayList<>(inputFunctionDecls);
        if (expression == null) {
            return functionDecls;
        }
        String value = (String) metadataProvider.getConfig().get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS);
        boolean includePrivateFunctions = (value != null) ? Boolean.valueOf(value.toLowerCase()) : false;
        Set<CallExpr> functionCalls = functionCollector.getFunctionCalls(expression);
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature signature = functionCall.getFunctionSignature();
            if (declaredFunctions != null && declaredFunctions.contains(signature)) {
                continue;
            }
            if (signature.getNamespace() == null) {
                signature.setNamespace(metadataProvider.getDefaultDataverseName());
            }
            String namespace = signature.getNamespace();
            // Checks the existence of the referred dataverse.
            try {
                if (!namespace.equals(FunctionConstants.ASTERIX_NS)
                        && !namespace.equals(AlgebricksBuiltinFunctions.ALGEBRICKS_NS)
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

            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)
                    || function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_SQLPP)) {
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

    public static List<List<List<String>>> getFunctionDependencies(IQueryRewriter rewriter, Expression expression,
            MetadataProvider metadataProvider) throws CompilationException {
        Set<CallExpr> functionCalls = rewriter.getFunctionCalls(expression);
        //Get the List of used functions and used datasets
        List<List<String>> datasourceDependencies = new ArrayList<>();
        List<List<String>> functionDependencies = new ArrayList<>();
        for (CallExpr functionCall : functionCalls) {
            FunctionSignature signature = functionCall.getFunctionSignature();
            FunctionIdentifier fid =
                    new FunctionIdentifier(signature.getNamespace(), signature.getName(), signature.getArity());
            if (fid.equals(BuiltinFunctions.DATASET)) {
                Pair<String, String> path = DatasetUtil.getDatasetInfo(metadataProvider,
                        ((LiteralExpr) functionCall.getExprList().get(0)).getValue().getStringValue());
                datasourceDependencies.add(Arrays.asList(path.first, path.second));
            }

            else if (BuiltinFunctions.isBuiltinCompilerFunction(signature, false)) {
                continue;
            } else {
                functionDependencies.add(Arrays.asList(signature.getNamespace(), signature.getName(),
                        Integer.toString(signature.getArity())));
            }
        }
        List<List<List<String>>> dependencies = new ArrayList<>();
        dependencies.add(datasourceDependencies);
        dependencies.add(functionDependencies);
        return dependencies;
    }

    private static Function lookupUserDefinedFunctionDecl(MetadataTransactionContext mdTxnCtx,
            FunctionSignature signature) throws AlgebricksException {
        if (signature.getNamespace() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

}
