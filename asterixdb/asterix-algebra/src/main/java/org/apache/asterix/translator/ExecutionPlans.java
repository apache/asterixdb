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

import java.io.Serializable;

public class ExecutionPlans implements Serializable {
    private static final long serialVersionUID = 6853904213354224458L;

    private String expressionTree;
    private String rewrittenExpressionTree;
    private String logicalPlan;
    private String optimizedLogicalPlan;
    private String job;
    private String signature;
    private String statementCategory;
    private String statementParameters;
    private boolean explainOnly;

    public ExecutionPlans() {
    }

    /** Copies a statement's plans, so they can be reported with it while the live plans go on changing. */
    public ExecutionPlans(ExecutionPlans other) {
        expressionTree = other.expressionTree;
        rewrittenExpressionTree = other.rewrittenExpressionTree;
        logicalPlan = other.logicalPlan;
        optimizedLogicalPlan = other.optimizedLogicalPlan;
        job = other.job;
        signature = other.signature;
        statementCategory = other.statementCategory;
        statementParameters = other.statementParameters;
        explainOnly = other.explainOnly;
    }

    /** Forgets the plans of the statement that just finished, so the next one does not report them as its own. */
    /**
     * Puts back the plans of {@code accumulated} that the statement just run did not produce, so that a request
     * reporting itself as a whole reports what it always did: each statement overwriting the fields it produces.
     */
    public void restoreMissingFrom(ExecutionPlans accumulated) {
        if (expressionTree == null) {
            expressionTree = accumulated.expressionTree;
        }
        if (rewrittenExpressionTree == null) {
            rewrittenExpressionTree = accumulated.rewrittenExpressionTree;
        }
        if (logicalPlan == null) {
            logicalPlan = accumulated.logicalPlan;
        }
        if (optimizedLogicalPlan == null) {
            optimizedLogicalPlan = accumulated.optimizedLogicalPlan;
        }
        if (job == null) {
            job = accumulated.job;
        }
        if (signature == null) {
            signature = accumulated.signature;
        }
        if (statementCategory == null) {
            statementCategory = accumulated.statementCategory;
        }
        if (statementParameters == null) {
            statementParameters = accumulated.statementParameters;
        }
        explainOnly |= accumulated.explainOnly;
    }

    public void clear() {
        expressionTree = null;
        rewrittenExpressionTree = null;
        logicalPlan = null;
        optimizedLogicalPlan = null;
        job = null;
        signature = null;
        statementCategory = null;
        statementParameters = null;
        explainOnly = false;
    }

    public String getExpressionTree() {
        return expressionTree;
    }

    public void setExpressionTree(String expressionTree) {
        this.expressionTree = expressionTree;
    }

    public String getRewrittenExpressionTree() {
        return rewrittenExpressionTree;
    }

    public void setRewrittenExpressionTree(String rewrittenExpressionTree) {
        this.rewrittenExpressionTree = rewrittenExpressionTree;
    }

    public String getLogicalPlan() {
        return logicalPlan;
    }

    public void setLogicalPlan(String logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public String getOptimizedLogicalPlan() {
        return optimizedLogicalPlan;
    }

    public void setOptimizedLogicalPlan(String optimizedLogicalPlan) {
        this.optimizedLogicalPlan = optimizedLogicalPlan;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getStatementCategory() {
        return statementCategory;
    }

    public void setStatementCategory(String statementCategory) {
        this.statementCategory = statementCategory;
    }

    public String getStatementParameters() {
        return statementParameters;
    }

    public void setStatementParameters(String statementParameters) {
        this.statementParameters = statementParameters;
    }

    public boolean isExplainOnly() {
        return explainOnly;
    }

    public void setExplainOnly(boolean explainOnly) {
        this.explainOnly = explainOnly;
    }
}
