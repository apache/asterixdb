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

package org.apache.asterix.api.http.server;

import java.util.Map;

import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class QueryServiceRequestParameters {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String host;
    private String path;
    private String statement;
    private String format;
    private String timeout;
    private boolean pretty;
    private String clientContextID;
    private String mode;
    private String maxResultReads;
    private String planFormat;
    private Map<String, JsonNode> statementParams;
    private boolean expressionTree;
    private boolean parseOnly; //don't execute; simply check for syntax correctness and named parameters.
    private boolean rewrittenExpressionTree;
    private boolean logicalPlan;
    private boolean optimizedLogicalPlan;
    private boolean job;
    private boolean signature;
    private boolean multiStatement;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    public boolean isPretty() {
        return pretty;
    }

    public void setPretty(boolean pretty) {
        this.pretty = pretty;
    }

    public String getClientContextID() {
        return clientContextID;
    }

    public void setClientContextID(String clientContextID) {
        this.clientContextID = clientContextID;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getMaxResultReads() {
        return maxResultReads;
    }

    public void setMaxResultReads(String maxResultReads) {
        this.maxResultReads = maxResultReads;
    }

    public String getPlanFormat() {
        return planFormat;
    }

    public void setPlanFormat(String planFormat) {
        this.planFormat = planFormat;
    }

    public Map<String, JsonNode> getStatementParams() {
        return statementParams;
    }

    public void setStatementParams(Map<String, JsonNode> statementParams) {
        this.statementParams = statementParams;
    }

    public boolean isExpressionTree() {
        return expressionTree;
    }

    public void setExpressionTree(boolean expressionTree) {
        this.expressionTree = expressionTree;
    }

    public boolean isRewrittenExpressionTree() {
        return rewrittenExpressionTree;
    }

    public void setRewrittenExpressionTree(boolean rewrittenExpressionTree) {
        this.rewrittenExpressionTree = rewrittenExpressionTree;
    }

    public boolean isLogicalPlan() {
        return logicalPlan;
    }

    public void setLogicalPlan(boolean logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public boolean isOptimizedLogicalPlan() {
        return optimizedLogicalPlan;
    }

    public void setOptimizedLogicalPlan(boolean optimizedLogicalPlan) {
        this.optimizedLogicalPlan = optimizedLogicalPlan;
    }

    public void setParseOnly(boolean parseOnly) {
        this.parseOnly = parseOnly;
    }

    public boolean isParseOnly() {
        return parseOnly;
    }

    public boolean isJob() {
        return job;
    }

    public void setJob(boolean job) {
        this.job = job;
    }

    public boolean isSignature() {
        return signature;
    }

    public void setSignature(boolean signature) {
        this.signature = signature;
    }

    public boolean isMultiStatement() {
        return multiStatement;
    }

    public void setMultiStatement(boolean multiStatement) {
        this.multiStatement = multiStatement;
    }

    @Override
    public String toString() {
        try {
            ObjectNode on = OBJECT_MAPPER.createObjectNode();
            on.put("host", host);
            on.put("path", path);
            on.put("statement", statement != null ? JSONUtil.escape(new StringBuilder(), statement).toString() : null);
            on.put("pretty", pretty);
            on.put("mode", mode);
            on.put("clientContextID", clientContextID);
            on.put("format", format);
            on.put("timeout", timeout);
            on.put("maxResultReads", maxResultReads);
            on.put("planFormat", planFormat);
            on.put("expressionTree", expressionTree);
            on.put("rewrittenExpressionTree", rewrittenExpressionTree);
            on.put("logicalPlan", logicalPlan);
            on.put("optimizedLogicalPlan", optimizedLogicalPlan);
            on.put("job", job);
            on.put("signature", signature);
            on.put("multiStatement", multiStatement);
            if (statementParams != null) {
                for (Map.Entry<String, JsonNode> statementParam : statementParams.entrySet()) {
                    on.set('$' + statementParam.getKey(), statementParam.getValue());
                }
            }
            return OBJECT_MAPPER.writeValueAsString(on);
        } catch (JsonProcessingException e) {
            QueryServiceServlet.LOGGER.debug("unexpected exception marshalling {} instance to json", getClass(), e);
            return e.toString();
        }
    }
}
