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
package org.apache.asterix.lang.sqlpp.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class FunctionMapUtil {

    public static final String CONCAT = "concat";
    public static final String DISTINCT_AGGREGATE_SUFFIX = "-distinct";
    private final static String CORE_AGGREGATE_PREFIX = "strict_";
    // This is a transitional case. The ALT_CORE_AGGREGATE_PREFIX should be removed again.
    private final static String ALT_CORE_AGGREGATE_PREFIX = "coll_";
    private final static String CORE_SQL_AGGREGATE_PREFIX = "array_";
    private final static String INTERNAL_SQL_AGGREGATE_PREFIX = "sql-";

    /**
     * SQL-92 aggregate functions for which {@link #CORE_AGGREGATE_PREFIX} should be used instead of
     * {@link #CORE_SQL_AGGREGATE_PREFIX} when mapping to a core SQL++ function.
     * (i.e. SQL-92 aggregate functions that preserve NULLs)
     */
    private final static Set<String> CORE_AGGREGATE_PREFIX_FUNCTIONS = new HashSet<>();

    // Maps from a variable-arg SQL function names to an internal list-arg function name.
    private static final Map<String, String> LIST_INPUT_FUNCTION_MAP = new HashMap<>();

    static {
        CORE_AGGREGATE_PREFIX_FUNCTIONS.add(BuiltinFunctions.SCALAR_ARRAYAGG.getName());
        CORE_AGGREGATE_PREFIX_FUNCTIONS.add(BuiltinFunctions.SCALAR_ARRAYAGG_DISTINCT.getName());

        LIST_INPUT_FUNCTION_MAP.put(CONCAT, BuiltinFunctions.STRING_CONCAT.getName());
        LIST_INPUT_FUNCTION_MAP.put("greatest", CORE_SQL_AGGREGATE_PREFIX + "max");
        LIST_INPUT_FUNCTION_MAP.put("least", CORE_SQL_AGGREGATE_PREFIX + "min");
    }

    /**
     * Whether a function signature is a SQL-92 core aggregate function.
     *
     * @param signature
     *            ,
     *            the function signature.
     * @return true if the function signature is a SQL-92 core aggregate,
     *         false otherwise.
     */
    public static boolean isSql92AggregateFunction(FunctionSignature signature) {
        String name = applySql92AggregateNameMapping(signature.getName().toLowerCase());
        IFunctionInfo finfo = FunctionUtil
                .getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS, name, signature.getArity()));
        if (finfo == null) {
            return false;
        }
        return BuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null;
    }

    /**
     * Get the corresponding SQL++ core aggregate function from the SQL-92 aggregate function.
     *
     * @param fs,
     *            the SQL-92 aggregate function signature.
     * @return the SQL++ aggregate function signature.
     */
    public static FunctionSignature sql92ToCoreAggregateFunction(FunctionSignature fs) {
        if (!isSql92AggregateFunction(fs)) {
            return fs;
        }
        String name = applySql92AggregateNameMapping(fs.getName().toLowerCase());
        String prefix =
                CORE_AGGREGATE_PREFIX_FUNCTIONS.contains(name) ? CORE_AGGREGATE_PREFIX : CORE_SQL_AGGREGATE_PREFIX;
        return new FunctionSignature(FunctionConstants.ASTERIX_NS, prefix + name, fs.getArity());
    }

    /**
     * Whether a function signature is a SQL++ core aggregate function.
     *
     * @param fs,
     *            the function signature.
     * @return true if the function signature is a SQL++ core aggregate,
     *         false otherwise.
     */
    public static boolean isCoreAggregateFunction(FunctionSignature fs) {
        String internalName = getInternalCoreAggregateFunctionName(fs);
        if (internalName != null) {
            FunctionIdentifier fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, internalName, fs.getArity());
            IFunctionInfo finfo = FunctionUtil.getFunctionInfo(fi);
            return finfo != null && BuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null;
        }
        return false;
    }

    /**
     * Maps a user invoked function signature to a system internal function signature.
     *
     * @param fs
     *            the user typed function.
     * @param checkSql92Aggregate
     *            enable check if the function is a SQL-92 aggregate function
     * @param sourceLoc
     *            the source location of the function call
     * @return the system internal function.
     * @throws CompilationException
     *             if checkSql92Aggregate is true and the function is a SQL-92 aggregate function
     */
    public static FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs, boolean checkSql92Aggregate,
            SourceLocation sourceLoc) throws CompilationException {
        FunctionSignature ns = CommonFunctionMapUtil.normalizeBuiltinFunctionSignature(fs);
        String internalName = getInternalCoreAggregateFunctionName(ns);
        if (internalName != null) {
            FunctionIdentifier fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, internalName, ns.getArity());
            IFunctionInfo finfo = FunctionUtil.getFunctionInfo(fi);
            if (finfo != null && BuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null) {
                return new FunctionSignature(FunctionConstants.ASTERIX_NS, internalName, ns.getArity());
            }
        } else if (checkSql92Aggregate) {
            if (isSql92AggregateFunction(ns)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        fs.getName() + " is a SQL-92 aggregate function. The SQL++ core aggregate function "
                                + sql92ToCoreAggregateFunction(ns).getName()
                                + " could potentially express the intent.");
            } else if (getInternalWindowFunction(ns) != null) {
                throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_WINDOW_EXPRESSION, sourceLoc);
            }
        }
        return new FunctionSignature(ns.getNamespace(), ns.getName(), ns.getArity());
    }

    /**
     * Rewrites a variable-arg, user-surface function call into an internal, list-arg function.
     *
     * @param callExpr
     *            The input call expression.
     * @return a new call expression that calls the corresponding AsterixDB internal function.
     */
    public static CallExpr normalizedListInputFunctions(CallExpr callExpr) {
        FunctionSignature fs = callExpr.getFunctionSignature();
        String internalFuncName = LIST_INPUT_FUNCTION_MAP.get(fs.getName().toLowerCase());
        if (internalFuncName == null) {
            return callExpr;
        }
        callExpr.setFunctionSignature(new FunctionSignature(FunctionConstants.ASTERIX_NS, internalFuncName, 1));
        ListConstructor listConstr =
                new ListConstructor(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR, callExpr.getExprList());
        listConstr.setSourceLocation(callExpr.getSourceLocation());
        callExpr.setExprList(new ArrayList<>(Collections.singletonList(listConstr)));
        return callExpr;
    }

    /**
     * Removes the "array_", "strict_", or "coll_" prefix for user-facing SQL++ core aggregate function names.
     *
     * @param fs
     *            a user-facing SQL++ core aggregate function signature.
     * @return the AsterixDB internal function name for the aggregate function.
     */
    private static String getInternalCoreAggregateFunctionName(FunctionSignature fs) {
        String name = fs.getName().toLowerCase();
        if (name.startsWith(CORE_AGGREGATE_PREFIX)) {
            return name.substring(CORE_AGGREGATE_PREFIX.length());
        } else if (name.startsWith(ALT_CORE_AGGREGATE_PREFIX)) {
            return name.substring(ALT_CORE_AGGREGATE_PREFIX.length());
        } else if (name.startsWith(CORE_SQL_AGGREGATE_PREFIX)) {
            return INTERNAL_SQL_AGGREGATE_PREFIX + name.substring(CORE_SQL_AGGREGATE_PREFIX.length());
        } else {
            return null;
        }
    }

    /**
     * Returns an internal implementation function for a public window function,
     * or {@code null} if given function is not a public window function
     * @param signature function signature
     * @return said value
     */
    public static FunctionIdentifier getInternalWindowFunction(FunctionSignature signature) {
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                signature.getName().toLowerCase(), signature.getArity()));
        return finfo != null ? BuiltinFunctions.getWindowFunction(finfo.getFunctionIdentifier()) : null;
    }

    /**
     * Returns original function name for a SQL-92 aggregate function alias or given name if there is no mapping
     * defined for it
     */
    private static String applySql92AggregateNameMapping(String functionName) {
        boolean distinct = functionName.endsWith(DISTINCT_AGGREGATE_SUFFIX);
        if (distinct) {
            String mainName = CommonFunctionMapUtil.getFunctionMapping(
                    functionName.substring(0, functionName.length() - DISTINCT_AGGREGATE_SUFFIX.length()));
            if (mainName != null) {
                return mainName + DISTINCT_AGGREGATE_SUFFIX;
            }
        } else {
            String mainName = CommonFunctionMapUtil.getFunctionMapping(functionName);
            if (mainName != null) {
                return mainName;
            }
        }
        return functionName;
    }
}
