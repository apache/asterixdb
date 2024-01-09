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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.literal.FalseLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class FunctionUtil {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";
    //TODO(wyk) add Multiply and Add
    private static final Set<FunctionIdentifier> COMMUTATIVE_FUNCTIONS =
            Set.of(BuiltinFunctions.EQ, BuiltinFunctions.AND, BuiltinFunctions.OR);

    private static final DataverseName FN_DATASET_DATAVERSE_NAME =
            FunctionSignature.getDataverseName(BuiltinFunctions.DATASET);

    private static final String FN_DATASET_NAME = BuiltinFunctions.DATASET.getName();

    /**
     * @deprecated use {@link BuiltinFunctions#getBuiltinFunctionInfo(FunctionIdentifier)} instead
     */
    public static BuiltinFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return BuiltinFunctions.getBuiltinFunctionInfo(fi);
    }

    public static TypeSignature getTypeDependencyFromFunctionParameter(TypeExpression typeExpr, String defaultDatabase,
            DataverseName defaultDataverse) {
        switch (typeExpr.getTypeKind()) {
            case ORDEREDLIST:
                return getTypeDependencyFromFunctionParameter(
                        ((OrderedListTypeDefinition) typeExpr).getItemTypeExpression(), defaultDatabase,
                        defaultDataverse);
            case UNORDEREDLIST:
                return getTypeDependencyFromFunctionParameter(
                        ((UnorderedListTypeDefinition) typeExpr).getItemTypeExpression(), defaultDatabase,
                        defaultDataverse);
            case TYPEREFERENCE:
                TypeReferenceExpression typeRef = ((TypeReferenceExpression) typeExpr);
                String typeName = typeRef.getIdent().getSecond().toString();
                BuiltinType builtinType = BuiltinTypeMap.getBuiltinType(typeName);
                if (builtinType != null) {
                    return null;
                }
                Namespace typeRefNamespace = typeRef.getIdent().getFirst();
                DataverseName typeDataverseName;
                String typeDatabaseName;
                if (typeRefNamespace == null) {
                    typeDataverseName = defaultDataverse;
                    typeDatabaseName = defaultDatabase;
                } else {
                    typeDataverseName = typeRefNamespace.getDataverseName();
                    typeDatabaseName = typeRefNamespace.getDatabaseName();
                }
                return new TypeSignature(typeDatabaseName, typeDataverseName, typeName);
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
        String database = fs.getDatabaseName();
        if (dataverse == null) {
            Namespace defaultNamespace = metadataProvider.getDefaultNamespace();
            dataverse = defaultNamespace.getDataverseName();
            database = defaultNamespace.getDatabaseName();
        }
        if (searchUdfs && !isBuiltinFunctionDataverse(dataverse)) {
            // attempt to resolve to a user-defined function
            FunctionSignature fsWithDv = fs.getDataverseName() == null
                    ? new FunctionSignature(database, dataverse, fs.getName(), fs.getArity()) : fs;
            FunctionSignature fsWithDvVarargs = new FunctionSignature(fsWithDv.getDatabaseName(),
                    fsWithDv.getDataverseName(), fsWithDv.getName(), FunctionIdentifier.VARARGS);

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
                    dv = metadataProvider.findDataverse(database, dataverse);
                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, sourceLoc,
                            MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
                }
                if (dv == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                            MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
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

    public static List<List<DependencyFullyQualifiedName>> getFunctionDependencies(FunctionDecl fd,
            IQueryRewriter rewriter) throws CompilationException {
        Expression normBody = fd.getNormalizedFuncBody();
        if (normBody == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fd.getSourceLocation(),
                    fd.getSignature().toString());
        }

        // Get the list of used functions and used datasets
        List<DependencyFullyQualifiedName> datasetDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> synonymDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(normBody, rewriter, datasetDependencies, synonymDependencies,
                functionDependencies);

        List<DependencyFullyQualifiedName> typeDependencies = Collections.emptyList();
        return Function.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    public static List<List<DependencyFullyQualifiedName>> getExternalFunctionDependencies(
            Collection<TypeSignature> dependentTypes) {
        List<DependencyFullyQualifiedName> datasetDependencies = Collections.emptyList();
        List<DependencyFullyQualifiedName> functionDependencies = Collections.emptyList();
        List<DependencyFullyQualifiedName> typeDependencies = new ArrayList<>(dependentTypes.size());
        List<DependencyFullyQualifiedName> synonymDependencies = Collections.emptyList();
        for (TypeSignature t : dependentTypes) {
            typeDependencies.add(
                    new DependencyFullyQualifiedName(t.getDatabaseName(), t.getDataverseName(), t.getName(), null));
        }
        return Function.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    public static boolean isBuiltinDatasetFunction(FunctionSignature fs) {
        return Objects.equals(FN_DATASET_DATAVERSE_NAME, fs.getDataverseName())
                && Objects.equals(FN_DATASET_NAME, fs.getName());
    }

    public static Triple<DatasetFullyQualifiedName, Boolean, DatasetFullyQualifiedName> parseDatasetFunctionArguments(
            AbstractCallExpression datasetFn) throws CompilationException {
        List<Expression> argList = datasetFn.getExprList();
        DatasetFullyQualifiedName datasetOrViewName = parseDatasetFunctionArguments(argList, 0,
                datasetFn.getSourceLocation(), ExpressionUtils::getStringLiteral);
        boolean isView = argList.size() > 3 && Boolean.TRUE.equals(ExpressionUtils.getBooleanLiteral(argList.get(3)));
        DatasetFullyQualifiedName synonymName = argList.size() > 4 ? parseDatasetFunctionArguments(argList, 4,
                datasetFn.getSourceLocation(), ExpressionUtils::getStringLiteral) : null;
        return new Triple<>(datasetOrViewName, isView, synonymName);
    }

    public static DatasetFullyQualifiedName parseDatasetFunctionArguments(AbstractFunctionCallExpression datasetFn)
            throws CompilationException {
        return parseDatasetFunctionArguments(datasetFn.getArguments(), 0, datasetFn.getSourceLocation(),
                FunctionUtil::getStringConstant);
    }

    private static <T> DatasetFullyQualifiedName parseDatasetFunctionArguments(List<T> datasetFnArgs, int startPos,
            SourceLocation sourceLoc, java.util.function.Function<T, String> stringAccessor)
            throws CompilationException {
        String databaseName = stringAccessor.apply(datasetFnArgs.get(startPos));
        if (databaseName == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        String dataverseNameArg = stringAccessor.apply(datasetFnArgs.get(startPos + 1));
        if (dataverseNameArg == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        DataverseName dataverseName;
        try {
            dataverseName = DataverseName.createFromCanonicalForm(dataverseNameArg);
        } catch (AsterixException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, e, "Invalid argument to dataset()");
        }
        String datasetName = stringAccessor.apply(datasetFnArgs.get(startPos + 2));
        if (datasetName == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Invalid argument to dataset()");
        }
        return new DatasetFullyQualifiedName(databaseName, dataverseName, datasetName);
    }

    private static String getStringConstant(Mutable<ILogicalExpression> arg) {
        return ConstantExpressionUtil.getStringConstant(arg.getValue());
    }

    private static boolean getImportPrivateFunctions(MetadataProvider metadataProvider) {
        String value = (String) metadataProvider.getConfig().get(IMPORT_PRIVATE_FUNCTIONS);
        return (value != null) && Boolean.parseBoolean(value.toLowerCase());
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

    public static boolean isFieldAccessFunction(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        return BuiltinFunctions.FIELD_ACCESS_BY_INDEX.equals(fid) || BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fid)
                || BuiltinFunctions.FIELD_ACCESS_NESTED.equals(fid);
    }

    /**
     * Compares two commutative expressions
     * TODO It doesn't support add and multiply (e.g., add(x, add(y, z) & add(add(x, y), z) will return false)
     *
     * @param expr1 left expression
     * @param expr2 right expression
     * @return true if equals, false otherwise
     */
    public static boolean commutativeEquals(ILogicalExpression expr1, ILogicalExpression expr2) {
        if (expr1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL
                || expr2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr1.equals(expr2);
        }

        AbstractFunctionCallExpression funcExpr1 = (AbstractFunctionCallExpression) expr1;
        AbstractFunctionCallExpression funcExpr2 = (AbstractFunctionCallExpression) expr2;

        FunctionIdentifier fid1 = funcExpr1.getFunctionIdentifier();
        FunctionIdentifier fid2 = funcExpr2.getFunctionIdentifier();

        if (!fid1.equals(fid2) || funcExpr1.getArguments().size() != funcExpr2.getArguments().size()) {
            return false;
        } else if (!COMMUTATIVE_FUNCTIONS.contains(fid1)) {
            return expr1.equals(expr2);
        }

        return commutativeEquals(expr1, expr2, new BitSet());
    }

    private static boolean commutativeEquals(ILogicalExpression expr1, ILogicalExpression expr2, BitSet matched) {
        if (expr1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL
                || expr2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr1.equals(expr2);
        }

        AbstractFunctionCallExpression funcExpr1 = (AbstractFunctionCallExpression) expr1;
        AbstractFunctionCallExpression funcExpr2 = (AbstractFunctionCallExpression) expr2;

        List<Mutable<ILogicalExpression>> args1 = funcExpr1.getArguments();
        List<Mutable<ILogicalExpression>> args2 = funcExpr2.getArguments();

        BitSet childrenSet = new BitSet();
        int numberOfMatches = 0;
        for (Mutable<ILogicalExpression> arg1 : args1) {
            int prevNumberOfMatches = numberOfMatches;

            for (int i = 0; i < args2.size(); i++) {
                Mutable<ILogicalExpression> arg2 = args2.get(i);
                childrenSet.clear();
                if (!matched.get(i) && commutativeEquals(arg1.getValue(), arg2.getValue(), childrenSet)) {
                    matched.set(i);
                    numberOfMatches++;
                    break;
                }
            }

            if (numberOfMatches == prevNumberOfMatches) {
                // Early exit as one operand didn't match with any of the other operands
                return false;
            }
        }

        return numberOfMatches == args1.size();
    }

    public static CallExpr makeDatasetCallExpr(String database, DataverseName dataverse, String dataset) {
        List<Expression> arguments = new ArrayList<>();
        addDataset(arguments, database, dataverse, dataset);
        return new CallExpr(new FunctionSignature(BuiltinFunctions.DATASET), arguments);
    }

    public static CallExpr makeDatasetCallExpr(String database, DataverseName dataverse, String dataset, boolean view) {
        List<Expression> argList = new ArrayList<>(4);
        addDataset(argList, database, dataverse, dataset);
        argList.add(new LiteralExpr(view ? TrueLiteral.INSTANCE : FalseLiteral.INSTANCE));
        return new CallExpr(new FunctionSignature(BuiltinFunctions.DATASET), argList);
    }

    public static CallExpr makeSynonymDatasetCallExpr(String resolvedDatabaseName, DataverseName resolvedDataverseName,
            String resolvedDatasetName, boolean isView, String databaseName, DataverseName dataverseName,
            String datasetName) {
        List<Expression> argList = new ArrayList<>(7);
        addDataset(argList, resolvedDatabaseName, resolvedDataverseName, resolvedDatasetName);
        argList.add(new LiteralExpr(isView ? TrueLiteral.INSTANCE : FalseLiteral.INSTANCE));
        addDataset(argList, databaseName, dataverseName, datasetName);
        return new CallExpr(new FunctionSignature(BuiltinFunctions.DATASET), argList);
    }

    private static void addDataset(List<Expression> argList, String db, DataverseName dv, String ds) {
        argList.add(new LiteralExpr(new StringLiteral(db)));
        argList.add(new LiteralExpr(new StringLiteral(dv.getCanonicalForm())));
        argList.add(new LiteralExpr(new StringLiteral(ds)));
    }
}
