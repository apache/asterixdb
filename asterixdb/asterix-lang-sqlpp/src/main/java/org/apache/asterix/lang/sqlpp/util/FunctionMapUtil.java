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
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionMapUtil {

    private final static String CORE_AGGREGATE_PREFIX = "coll_";
    private final static String SQL_PREFIX = "sql-";

    // Maps from a SQL function name to an AQL function name (i.e., AsterixDB internal name).
    private static final Map<String, String> FUNCTION_NAME_MAP = new HashMap<>();

    static {
        FUNCTION_NAME_MAP.put("ceil", "ceiling"); //SQL: ceil,  AQL: ceiling
        FUNCTION_NAME_MAP.put("length", "string-length"); // SQL: length,  AQL: string-length
        FUNCTION_NAME_MAP.put("lower", "lowercase"); // SQL: lower, AQL: lowercase
        FUNCTION_NAME_MAP.put("substr", "substring"); // SQL: substr,  AQL: substring
        FUNCTION_NAME_MAP.put("upper", "uppercase"); //SQL: upper, AQL: uppercase
        FUNCTION_NAME_MAP.put("title", "initcap"); //SQL: title, SQL/AQL: initcap
        FUNCTION_NAME_MAP.put("regexp_contains", "matches"); //SQL: regexp_contains, AQL: matches
        FUNCTION_NAME_MAP.put("regexp_like", "regexp-like"); //SQL: regexp_like, AQL: regexp-like
        FUNCTION_NAME_MAP.put("regexp_position", "regexp-position"); //SQL: regexp_position, AQL: regexp-position
        FUNCTION_NAME_MAP.put("regexp_replace", "replace"); //SQL: regexp_replace, AQL: replace
        FUNCTION_NAME_MAP.put("power", "caret"); //SQL: pow, AQL: caret
    }

    // Maps from a variable-arg SQL function names to an internal list-arg function name.
    private static final Map<String, String> LIST_INPUT_FUNCTION_MAP = new HashMap<>();

    static {
        LIST_INPUT_FUNCTION_MAP.put("concat", "string-concat");
        LIST_INPUT_FUNCTION_MAP.put("greatest", CORE_AGGREGATE_PREFIX + SQL_PREFIX + "max");
        LIST_INPUT_FUNCTION_MAP.put("least", CORE_AGGREGATE_PREFIX + SQL_PREFIX + "min");
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
    public static boolean isSql92AggregateFunction(FunctionSignature signature) throws AsterixException {
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                signature.getName().toLowerCase(), signature.getArity()));
        if (finfo == null) {
            return false;
        }
        return AsterixBuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null;
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
        String name = fs.getName().toLowerCase();
        if (!name.startsWith(CORE_AGGREGATE_PREFIX)) {
            return false;
        }
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                name.substring(CORE_AGGREGATE_PREFIX.length()), fs.getArity()));
        if (finfo == null) {
            return false;
        }
        return AsterixBuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null;
    }

    /**
     * Get the corresponding SQL++ core aggregate function from the SQL-92 aggregate function.
     *
     * @param fs,
     *            the SQL-92 aggregate function signature.
     * @return the SQL++ aggregate function signature.
     * @throws AsterixException
     */
    public static FunctionSignature sql92ToCoreAggregateFunction(FunctionSignature fs) throws AsterixException {
        if (!isSql92AggregateFunction(fs)) {
            return fs;
        }
        return new FunctionSignature(fs.getNamespace(), CORE_AGGREGATE_PREFIX + SQL_PREFIX + fs.getName(),
                fs.getArity());
    }

    /**
     * Maps a user invoked function signature to a system internal function signature.
     *
     * @param fs,
     *            the user typed function.
     * @return the system internal function.
     */
    public static FunctionSignature normalizeBuiltinFunctionSignature(FunctionSignature fs, boolean checkSql92Aggregate)
            throws AsterixException {
        String mappedName = internalizeBuiltinScalarFunctionName(fs.getName());
        if (isCoreAggregateFunction(fs)) {
            mappedName = internalizeCoreAggregateFunctionName(mappedName);
        } else if (checkSql92Aggregate && isSql92AggregateFunction(fs)) {
            throw new AsterixException(fs.getName()
                    + " is a SQL-92 aggregate function. The SQL++ core aggregate function " + CORE_AGGREGATE_PREFIX
                    + fs.getName().toLowerCase() + " could potentially express the intent.");
        }
        return new FunctionSignature(fs.getNamespace(), mappedName, fs.getArity());
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
        callExpr.setExprList(new ArrayList<>(Collections.singletonList(new ListConstructor(
                ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR, callExpr.getExprList()))));
        return callExpr;
    }

    /**
     * Removes the "coll_" prefix for user-facing SQL++ core aggregate function names.
     *
     * @param name,
     *            the name of a user-facing SQL++ core aggregate function name.
     * @return the AsterixDB internal function name for the aggregate function.
     * @throws AsterixException
     */
    private static String internalizeCoreAggregateFunctionName(String name) throws AsterixException {
        String lowerCaseName = name.toLowerCase();
        return lowerCaseName.substring(CORE_AGGREGATE_PREFIX.length());
    }

    /**
     * Note: function name normalization can ONLY be called
     * after all user-defined functions (by either "DECLARE FUNCTION" or "CREATE FUNCTION")
     * are inlined, because user-defined function names are case-sensitive.
     *
     * @param name
     *            the user-input function name in the query.
     * @return the mapped internal name.
     */
    private static String internalizeBuiltinScalarFunctionName(String name) {
        String lowerCaseName = name.toLowerCase();
        String mappedName = FUNCTION_NAME_MAP.get(lowerCaseName);
        if (mappedName != null) {
            return mappedName;
        }
        return lowerCaseName;
    }
}
