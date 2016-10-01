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
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionMapUtil {

    private final static String CORE_AGGREGATE_PREFIX = "coll_";
    private final static String CORE_SQL_AGGREGATE_PREFIX = "array_";
    private final static String INTERNAL_SQL_AGGREGATE_PREFIX = "sql-";

    // Maps from a variable-arg SQL function names to an internal list-arg function name.
    private static final Map<String, String> LIST_INPUT_FUNCTION_MAP = new HashMap<>();

    static {
        LIST_INPUT_FUNCTION_MAP.put("concat", "string-concat");
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
        boolean coreAgg = name.startsWith(CORE_AGGREGATE_PREFIX);
        boolean coreSqlAgg = name.startsWith(CORE_SQL_AGGREGATE_PREFIX);
        if (!coreAgg && !coreSqlAgg) {
            return false;
        }
        String internalName = coreAgg ? name.substring(CORE_AGGREGATE_PREFIX.length())
                : (INTERNAL_SQL_AGGREGATE_PREFIX + name.substring(CORE_SQL_AGGREGATE_PREFIX.length()));
        IFunctionInfo finfo = FunctionUtil
                .getFunctionInfo(new FunctionIdentifier(FunctionConstants.ASTERIX_NS, internalName, fs.getArity()));
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
        return new FunctionSignature(fs.getNamespace(), CORE_SQL_AGGREGATE_PREFIX + fs.getName(),
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
        if (isCoreAggregateFunction(fs)) {
            return internalizeCoreAggregateFunctionName(fs);
        } else if (checkSql92Aggregate && isSql92AggregateFunction(fs)) {
            throw new AsterixException(fs.getName()
                    + " is a SQL-92 aggregate function. The SQL++ core aggregate function " + CORE_SQL_AGGREGATE_PREFIX
                    + fs.getName().toLowerCase() + " could potentially express the intent.");
        }
        String mappedName = CommonFunctionMapUtil.normalizeBuiltinFunctionSignature(fs).getName();
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
     * Removes the "array_" prefix for user-facing SQL++ core aggregate function names.
     *
     * @param fs,
     *            a user-facing SQL++ core aggregate function signature.
     * @return the AsterixDB internal function signature for the aggregate function.
     * @throws AsterixException
     */
    private static FunctionSignature internalizeCoreAggregateFunctionName(FunctionSignature fs)
            throws AsterixException {
        String name = fs.getName().toLowerCase();
        boolean coreAgg = name.startsWith(CORE_AGGREGATE_PREFIX);
        String lowerCaseName = coreAgg ? name.substring(CORE_AGGREGATE_PREFIX.length())
                : (INTERNAL_SQL_AGGREGATE_PREFIX + name.substring(CORE_SQL_AGGREGATE_PREFIX.length()));
        return new FunctionSignature(fs.getNamespace(), lowerCaseName, fs.getArity());
    }

}
