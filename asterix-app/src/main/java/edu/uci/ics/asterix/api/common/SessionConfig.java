/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.common;

public class SessionConfig {
    private final boolean optimize;
    private final boolean printExprParam;
    private final boolean printRewrittenExprParam;
    private final boolean printLogicalPlanParam;
    private final boolean printOptimizedLogicalPlanParam;
    private final boolean printPhysicalOpsOnly;
    private final boolean executeQuery;
    private final boolean generateJobSpec;
    private final boolean printJob;

    public SessionConfig(boolean optimize, boolean printExprParam, boolean printRewrittenExprParam,
            boolean printLogicalPlanParam, boolean printOptimizedLogicalPlanParam, boolean printPhysicalOpsOnly,
            boolean executeQuery, boolean generateJobSpec, boolean printJob) {
        this.optimize = optimize;
        this.printExprParam = printExprParam;
        this.printRewrittenExprParam = printRewrittenExprParam;
        this.printLogicalPlanParam = printLogicalPlanParam;
        this.printOptimizedLogicalPlanParam = printOptimizedLogicalPlanParam;
        this.printPhysicalOpsOnly = printPhysicalOpsOnly;
        this.executeQuery = executeQuery;
        this.generateJobSpec = generateJobSpec;
        this.printJob = printJob;
    }

    public boolean isPrintExprParam() {
        return printExprParam;
    }

    public boolean isPrintRewrittenExprParam() {
        return printRewrittenExprParam;
    }

    public boolean isPrintLogicalPlanParam() {
        return printLogicalPlanParam;
    }

    public boolean isPrintOptimizedLogicalPlanParam() {
        return printOptimizedLogicalPlanParam;
    }

    public boolean isPrintJob() {
        return printJob;
    }

    public boolean isPrintPhysicalOpsOnly() {
        return printPhysicalOpsOnly;
    }

    public boolean isExecuteQuery() {
        return executeQuery;
    }

    public boolean isOptimize() {
        return optimize;
    }

    public boolean isGenerateJobSpec() {
        return generateJobSpec;
    }
}