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
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtil {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(FunctionSignature fs) {
        return getFunctionInfo(new FunctionIdentifier(fs.getNamespace(), fs.getName(), fs.getArity()));
    }

    @FunctionalInterface
    public interface IFunctionCollector {
        Set<FunctionSignature> getFunctionCalls(Expression expression) throws AsterixException;
    }

    @FunctionalInterface
    public interface IFunctionParser {
        FunctionDecl getFunctionDecl(Function function) throws AsterixException;
    }

    @FunctionalInterface
    public interface IFunctionNormalizer {
        FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs) throws AsterixException;
    }

    /**
     * Retrieve stored functions (from CREATE FUNCTION statements) that have been used in an expression.
     *
     * @param metadataProvider,
     *            the metadata provider
     * @param expression,
     *            the expression for analysis
     * @param declaredFunctions,
     *            a set of declared functions in the query, which can potentially override stored functions.
     * @param functionCollector,
     *            for collecting function calls in the <code>expression</code>
     * @param functionParser,
     *            for parsing stored functions in the string represetnation.
     * @param functionNormalizer,
     *            for normalizing function names.
     * @throws AsterixException
     */
    public static List<FunctionDecl> retrieveUsedStoredFunctions(MetadataProvider metadataProvider,
            Expression expression, List<FunctionSignature> declaredFunctions, List<FunctionDecl> inputFunctionDecls,
            IFunctionCollector functionCollector, IFunctionParser functionParser,
            IFunctionNormalizer functionNormalizer) throws AsterixException {
        List<FunctionDecl> functionDecls = inputFunctionDecls == null ? new ArrayList<>()
                : new ArrayList<>(inputFunctionDecls);
        if (expression == null) {
            return functionDecls;
        }
        String value = metadataProvider.getConfig().get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS);
        boolean includePrivateFunctions = (value != null) ? Boolean.valueOf(value.toLowerCase()) : false;
        Set<FunctionSignature> functionCalls = functionCollector.getFunctionCalls(expression);
        for (FunctionSignature signature : functionCalls) {
            if (declaredFunctions != null && declaredFunctions.contains(signature)) {
                continue;
            }
            if (signature.getNamespace() == null) {
                signature.setNamespace(metadataProvider.getDefaultDataverseName());
            }
            String namespace = signature.getNamespace();
            // Checks the existence of the referred dataverse.
            if (metadataProvider.findDataverse(namespace) == null
                    && !namespace.equals(FunctionConstants.ASTERIX_NS)) {
                throw new AsterixException("In function call \"" + namespace + "." + signature.getName()
                        + "(...)\", the dataverse \"" + namespace + "\" cannot be found!");
            }
            Function function = lookupUserDefinedFunctionDecl(metadataProvider.getMetadataTxnContext(), signature);
            if (function == null) {
                FunctionSignature normalizedSignature = functionNormalizer == null ? signature
                        : functionNormalizer.normalizeBuiltinFunctionSignature(signature);
                if (AsterixBuiltinFunctions.isBuiltinCompilerFunction(normalizedSignature, includePrivateFunctions)) {
                    continue;
                }
                StringBuilder messageBuilder = new StringBuilder();
                if (!functionDecls.isEmpty()) {
                    messageBuilder.append("function " + functionDecls.get(functionDecls.size() - 1).getSignature()
                            + " depends upon function " + signature + " which is undefined");
                } else {
                    messageBuilder.append("function " + signature + " is not defined");
                }
                throw new AsterixException(messageBuilder.toString());
            }

            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                FunctionDecl functionDecl = functionParser.getFunctionDecl(function);
                if (functionDecl != null) {
                    if (functionDecls.contains(functionDecl)) {
                        throw new AsterixException(
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

    private static Function lookupUserDefinedFunctionDecl(MetadataTransactionContext mdTxnCtx,
            FunctionSignature signature) throws AsterixException {
        if (signature.getNamespace() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

}
