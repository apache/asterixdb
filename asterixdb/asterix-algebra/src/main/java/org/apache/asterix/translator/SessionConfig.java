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
package org.apache.asterix.translator;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;

/**
 * SessionConfig captures several different parameters for controlling
 * the execution of an APIFramework call.
 * <li>It specifies how the execution will proceed (for instance,
 * whether to optimize, or whether to execute at all).
 * <li>It allows you specify where the primary execution output will
 * be sent.
 * <li>It also allows you to request additional output for optional
 * out-of-band data about the execution (query plan, etc).
 * <li>It allows you to specify the output format for the primary
 * execution output - LOSSLESS_JSON, CSV, etc.
 * <li>It allows you to specify output format-specific parameters.
 */

public class SessionConfig {
    /**
     * Used to specify the output format for the primary execution.
     */
    public enum OutputFormat {
        ADM,
        CSV,
        CLEAN_JSON,
        LOSSLESS_JSON
    };

    /**
     * Produce out-of-band output for Hyracks Job.
     */
    public static final String OOB_HYRACKS_JOB = "oob-hyracks-job";

    /**
     * Produce out-of-band output for Expression Tree.
     */
    public static final String OOB_EXPR_TREE = "oob-expr-tree";

    /**
     * Produce out-of-band output for Rewritten Expression Tree.
     */
    public static final String OOB_REWRITTEN_EXPR_TREE = "oob-rewritten-expr-tree";

    /**
     * Produce out-of-band output for Logical Plan.
     */
    public static final String OOB_LOGICAL_PLAN = "oob-logical-plan";

    /**
     * Produce out-of-band output for Optimized Logical Plan.
     */
    public static final String OOB_OPTIMIZED_LOGICAL_PLAN = "oob-optimized-logical-plan";

    /**
     * Format flag: print only physical ops (for optimizer tests).
     */
    public static final String FORMAT_ONLY_PHYSICAL_OPS = "format-only-physical-ops";

    /**
     * Format flag: wrap out-of-band data in HTML.
     */
    public static final String FORMAT_HTML = "format-html";

    /**
     * Format flag: print CSV header line.
     */
    public static final String FORMAT_CSV_HEADER = "format-csv-header";

    /**
     * Format flag: wrap results in outer array brackets (JSON or ADM).
     */
    public static final String FORMAT_WRAPPER_ARRAY = "format-wrapper-array";

    /**
     * Format flag: indent JSON results.
     */
    public static final String FORMAT_INDENT_JSON = "indent-json";

    /**
     * Format flag: quote records in the results array.
     */
    public static final String FORMAT_QUOTE_RECORD = "quote-record";

    @FunctionalInterface
    public interface ResultDecorator {
        AlgebricksAppendable append(AlgebricksAppendable app) throws AlgebricksException;
    }

    // Standard execution flags.
    private final boolean executeQuery;
    private final boolean generateJobSpec;
    private final boolean optimize;

    // Output path for primary execution.
    private final PrintWriter out;

    // Output format.
    private final OutputFormat fmt;

    private final ResultDecorator preResultDecorator;
    private final ResultDecorator postResultDecorator;
    private final ResultDecorator preHandleDecorator;
    private final ResultDecorator postHandleDecorator;

    // Flags.
    private final Map<String, Boolean> flags;

    /**
     * Create a SessionConfig object with all default values:
     * - All format flags set to "false".
     * - All out-of-band outputs set to "null".
     * - "Optimize" set to "true".
     * - "Execute Query" set to "true".
     * - "Generate Job Spec" set to "true".
     *
     * @param out
     *            PrintWriter for execution output.
     * @param fmt
     *            Output format for execution output.
     */
    public SessionConfig(PrintWriter out, OutputFormat fmt) {
        this(out, fmt, null, null, null, null, true, true, true);
    }

    public SessionConfig(PrintWriter out, OutputFormat fmt, ResultDecorator preResultDecorator,
            ResultDecorator postResultDecorator, ResultDecorator preHandleDecorator,
            ResultDecorator postHandleDecorator) {
        this(out, fmt, preResultDecorator, postResultDecorator, preHandleDecorator, postHandleDecorator, true, true,
                true);
    }

    public SessionConfig(PrintWriter out, OutputFormat fmt, boolean optimize, boolean executeQuery,
            boolean generateJobSpec) {
        this(out, fmt, null, null, null, null, optimize, executeQuery, generateJobSpec);
    }

    /**
     * Create a SessionConfig object with all optional values set to defaults:
     * - All format flags set to "false".
     * - All out-of-band outputs set to "false".
     *
     * @param out
     *            PrintWriter for execution output.
     * @param fmt
     *            Output format for execution output.
     * @param optimize
     *            Whether to optimize the execution.
     * @param executeQuery
     *            Whether to execute the query or not.
     * @param generateJobSpec
     *            Whether to generate the Hyracks job specification (if
     *            false, job cannot be executed).
     */
    public SessionConfig(PrintWriter out, OutputFormat fmt, ResultDecorator preResultDecorator,
            ResultDecorator postResultDecorator, ResultDecorator preHandleDecorator,
            ResultDecorator postHandleDecorator, boolean optimize, boolean executeQuery, boolean generateJobSpec) {
        this.out = out;
        this.fmt = fmt;
        this.preResultDecorator = preResultDecorator;
        this.postResultDecorator = postResultDecorator;
        this.preHandleDecorator = preHandleDecorator;
        this.postHandleDecorator = postHandleDecorator;
        this.optimize = optimize;
        this.executeQuery = executeQuery;
        this.generateJobSpec = generateJobSpec;
        this.flags = new HashMap<>();
    }

    /**
     * Retrieve the PrintWriter to produce output to.
     */
    public PrintWriter out() {
        return this.out;
    }

    /**
     * Retrieve the OutputFormat for this execution.
     */
    public OutputFormat fmt() {
        return this.fmt;
    }

    public AlgebricksAppendable resultPrefix(AlgebricksAppendable app) throws AlgebricksException {
        return this.preResultDecorator != null ? this.preResultDecorator.append(app) : app;
    }

    public AlgebricksAppendable resultPostfix(AlgebricksAppendable app) throws AlgebricksException {
        return this.postResultDecorator != null ? this.postResultDecorator.append(app) : app;
    }

    public AlgebricksAppendable handlePrefix(AlgebricksAppendable app) throws AlgebricksException {
        return this.preHandleDecorator != null ? this.preHandleDecorator.append(app) : app;
    }

    public AlgebricksAppendable handlePostfix(AlgebricksAppendable app) throws AlgebricksException {
        return this.postHandleDecorator != null ? this.postHandleDecorator.append(app) : app;
    }
    /**
     * Retrieve the value of the "execute query" flag.
     */
    public boolean isExecuteQuery() {
        return executeQuery;
    }

    /**
     * Retrieve the value of the "optimize" flag.
     */
    public boolean isOptimize() {
        return optimize;
    }

    /**
     * Retrieve the value of the "generate job spec" flag.
     */
    public boolean isGenerateJobSpec() {
        return generateJobSpec;
    }

    /**
     * Specify all out-of-band settings at once. For convenience of older code.
     */
    public void setOOBData(boolean exprTree, boolean rewrittenExprTree, boolean logicalPlan,
            boolean optimizedLogicalPlan, boolean hyracksJob) {
        this.set(OOB_EXPR_TREE, exprTree);
        this.set(OOB_REWRITTEN_EXPR_TREE, rewrittenExprTree);
        this.set(OOB_LOGICAL_PLAN, logicalPlan);
        this.set(OOB_OPTIMIZED_LOGICAL_PLAN, optimizedLogicalPlan);
        this.set(OOB_HYRACKS_JOB, hyracksJob);
    }

    /**
     * Specify a flag.
     *
     * @param flag
     *            One of the OOB_ or FORMAT_ constants from this class.
     * @param value
     *            Value for the flag (all flags default to "false").
     */
    public void set(String flag, boolean value) {
        flags.put(flag, Boolean.valueOf(value));
    }

    /**
     * Retrieve the setting of a format-specific flag.
     *
     * @param flag
     *            One of the FORMAT_ constants from this class.
     * @returns true or false (all flags default to "false").
     */
    public boolean is(String flag) {
        Boolean value = flags.get(flag);
        return value == null ? false : value.booleanValue();
    }
}
