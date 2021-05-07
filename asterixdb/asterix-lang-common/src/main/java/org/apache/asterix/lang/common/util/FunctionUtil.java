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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
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
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;

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

    public static FunctionSignature resolveFunctionCall(FunctionSignature fs, SourceLocation sourceLoc,
            MetadataProvider metadataProvider, BiFunction<String, Integer, FunctionSignature> builtinFunctionResolver,
            boolean searchUdfs, Map<FunctionSignature, FunctionDecl> declaredFunctionMap,
            boolean allowNonStoredUdfCalls) throws CompilationException {
        DataverseName dataverse = fs.getDataverseName();
        if (dataverse == null) {
            dataverse = metadataProvider.getDefaultDataverseName();
        }
        if (searchUdfs && !isBuiltinFunctionDataverse(dataverse)) {
            // attempt to resolve to a user-defined function
            FunctionSignature fsWithDv =
                    fs.getDataverseName() == null ? new FunctionSignature(dataverse, fs.getName(), fs.getArity()) : fs;
            FunctionSignature fsWithDvVarargs =
                    new FunctionSignature(fsWithDv.getDataverseName(), fsWithDv.getName(), FunctionIdentifier.VARARGS);

            FunctionDecl fd = declaredFunctionMap.get(fsWithDv);
            if (fd == null) {
                fd = declaredFunctionMap.get(fsWithDvVarargs);
            }
            if (fd != null) {
                if (!allowNonStoredUdfCalls && !fd.isStored()) {
                    throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_USE, sourceLoc,
                            fd.getSignature().toString());
                }
                return fd.getSignature();
            }
            try {
                Function fn = metadataProvider.lookupUserDefinedFunction(fsWithDv);
                if (fn == null) {
                    fn = metadataProvider.lookupUserDefinedFunction(fsWithDvVarargs);
                }
                if (fn != null) {
                    return fn.getSignature();
                }
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, e, sourceLoc, fs.toString());
            }
            // fail if the dataverse was specified in the function call but this dataverse does not exist
            if (fs.getDataverseName() != null) {
                Dataverse dv;
                try {
                    dv = metadataProvider.findDataverse(dataverse);
                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, sourceLoc, dataverse);
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
        FunctionSignature fsBuiltin = builtinFunctionResolver.apply(name, fs.getArity());
        if (fsBuiltin == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, fs.toString());
        }
        return fsBuiltin;
    }

    public static boolean isBuiltinFunctionSignature(FunctionSignature fs) {
        return isBuiltinFunctionDataverse(Objects.requireNonNull(fs.getDataverseName()))
                || BuiltinFunctions.getBuiltinFunctionInfo(fs.createFunctionIdentifier()) != null;
    }

    private static boolean isBuiltinFunctionDataverse(DataverseName dataverse) {
        return FunctionConstants.ASTERIX_DV.equals(dataverse) || FunctionConstants.ALGEBRICKS_DV.equals(dataverse);
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

    public static void checkFunctionRecursion(Map<FunctionSignature, FunctionDecl> functionDeclMap,
            java.util.function.Function<Collection<AbstractCallExpression>, GatherFunctionCallsVisitor> gfcFactory,
            SourceLocation sourceLoc) throws CompilationException {
        List<AbstractCallExpression> callList = new ArrayList<>();
        GatherFunctionCallsVisitor gfc = gfcFactory.apply(callList);
        MutableGraph<FunctionDecl> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
        for (FunctionDecl fdFrom : functionDeclMap.values()) {
            callList.clear();
            fdFrom.getNormalizedFuncBody().accept(gfc, null);
            for (AbstractCallExpression callExpr : callList) {
                if (callExpr.getKind() == Expression.Kind.CALL_EXPRESSION) {
                    FunctionSignature callSignature = callExpr.getFunctionSignature();
                    FunctionDecl fdTo = functionDeclMap.get(callSignature);
                    if (fdTo != null) {
                        graph.putEdge(fdFrom, fdTo);
                    }
                }
            }
        }
        if (Graphs.hasCycle(graph)) {
            throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_RECURSION, sourceLoc);
        }
    }

    public static List<List<Triple<DataverseName, String, String>>> getFunctionDependencies(IQueryRewriter rewriter,
            Expression expression) throws CompilationException {
        List<AbstractCallExpression> functionCalls = new ArrayList<>();
        rewriter.getFunctionCalls(expression, functionCalls);
        // Duplicate elimination
        Set<FunctionSignature> seenFunctions = new HashSet<>();
        Set<Pair<DataverseName, String>> seenDatasets = new HashSet<>();
        Set<Pair<DataverseName, String>> seenSynonyms = new HashSet<>();
        //Get the List of used functions and used datasets
        List<Triple<DataverseName, String, String>> datasetDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> typeDependencies = Collections.emptyList();
        List<Triple<DataverseName, String, String>> synonymDependencies = new ArrayList<>();
        for (AbstractCallExpression functionCall : functionCalls) {
            switch (functionCall.getKind()) {
                case CALL_EXPRESSION:
                    FunctionSignature signature = functionCall.getFunctionSignature();
                    if (isBuiltinDatasetFunction(signature)) {
                        CallExpr callExpr = (CallExpr) functionCall;
                        if (callExpr.getExprList().size() > 2) {
                            // resolved via synonym -> store synonym name as a dependency
                            Pair<DataverseName, String> synonymReference = parseDatasetFunctionArguments(callExpr, 2);
                            if (seenSynonyms.add(synonymReference)) {
                                synonymDependencies
                                        .add(new Triple<>(synonymReference.first, synonymReference.second, null));
                            }
                        } else {
                            // resolved directly -> store dataset name as a dependency
                            Pair<DataverseName, String> datasetReference = parseDatasetFunctionArguments(callExpr, 0);
                            if (seenDatasets.add(datasetReference)) {
                                datasetDependencies
                                        .add(new Triple<>(datasetReference.first, datasetReference.second, null));
                            }
                        }
                    } else if (BuiltinFunctions.getBuiltinFunctionInfo(signature.createFunctionIdentifier()) == null) {
                        if (seenFunctions.add(signature)) {
                            functionDependencies.add(new Triple<>(signature.getDataverseName(), signature.getName(),
                                    Integer.toString(signature.getArity())));
                        }
                    }
                    break;
                case WINDOW_EXPRESSION:
                    // there cannot be used-defined window functions
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, expression.getSourceLocation(),
                            functionCall.getFunctionSignature().toString(false));
            }
        }
        return Function.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    public static List<List<Triple<DataverseName, String, String>>> getExternalFunctionDependencies(
            Collection<TypeSignature> dependentTypes) {
        List<Triple<DataverseName, String, String>> datasetDependencies = Collections.emptyList();
        List<Triple<DataverseName, String, String>> functionDependencies = Collections.emptyList();
        List<Triple<DataverseName, String, String>> typeDependencies = new ArrayList<>(dependentTypes.size());
        List<Triple<DataverseName, String, String>> synonymDependencies = Collections.emptyList();
        for (TypeSignature t : dependentTypes) {
            typeDependencies.add(new Triple<>(t.getDataverseName(), t.getName(), null));
        }
        return Function.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    public static boolean isBuiltinDatasetFunction(FunctionSignature fs) {
        return Objects.equals(FN_DATASET_DATAVERSE_NAME, fs.getDataverseName())
                && Objects.equals(FN_DATASET_NAME, fs.getName());
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(CallExpr datasetFn)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn, 0);
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(CallExpr datasetFn, int startPos)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn.getExprList(), startPos, datasetFn.getSourceLocation(),
                ExpressionUtils::getStringLiteral);
    }

    public static Pair<DataverseName, String> parseDatasetFunctionArguments(AbstractFunctionCallExpression datasetFn)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn.getArguments(), 0, datasetFn.getSourceLocation(),
                FunctionUtil::getStringConstant);
    }

    private static <T> Pair<DataverseName, String> parseDatasetFunctionArguments(List<T> datasetFnArgs, int startPos,
            SourceLocation sourceLoc, java.util.function.Function<T, String> argExtractFunction)
            throws CompilationException {
        String dataverseNameArg = argExtractFunction.apply(datasetFnArgs.get(startPos));
        if (dataverseNameArg == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        DataverseName dataverseName;
        try {
            dataverseName = DataverseName.createFromCanonicalForm(dataverseNameArg);
        } catch (AsterixException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, e, "Invalid argument to dataset()");
        }
        String datasetName = argExtractFunction.apply(datasetFnArgs.get(startPos + 1));
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

    public static Map<FunctionSignature, FunctionDecl> getFunctionMap(List<FunctionDecl> declaredFunctions) {
        if (declaredFunctions == null || declaredFunctions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<FunctionSignature, FunctionDecl> result = new HashMap<>();
        for (FunctionDecl fd : declaredFunctions) {
            result.put(fd.getSignature(), fd);
        }
        return result;
    }

    public static FunctionDecl parseStoredFunction(Function function, IParserFactory parserFactory,
            IWarningCollector warningCollector, SourceLocation sourceLoc) throws CompilationException {
        if (!function.getLanguage().equals(parserFactory.getLanguage())) {
            throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE, sourceLoc,
                    function.getLanguage(), function.getSignature().toString(), parserFactory.getLanguage());
        }
        IParser parser = parserFactory.createParser(new StringReader(function.getFunctionBody()));
        try {
            FunctionDecl functionDecl =
                    parser.parseFunctionBody(function.getSignature(), function.getParameterNames(), true);
            functionDecl.setSourceLocation(sourceLoc);
            if (warningCollector != null) {
                parser.getWarnings(warningCollector);
            }
            return functionDecl;
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_FUNCTION_DEFINITION, e, sourceLoc,
                    function.getSignature(), e.getMessage());
        }
    }
}
