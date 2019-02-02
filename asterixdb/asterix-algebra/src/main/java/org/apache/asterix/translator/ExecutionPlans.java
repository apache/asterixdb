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
    private static final long serialVersionUID = 6853904213354224457L;

    private String expressionTree;
    private String rewrittenExpressionTree;
    private String logicalPlan;
    private String optimizedLogicalPlan;
    private String job;

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
}
