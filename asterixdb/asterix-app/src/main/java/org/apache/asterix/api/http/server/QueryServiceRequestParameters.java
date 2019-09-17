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

    public static final long DEFAULT_MAX_WARNINGS = 0L;
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
    private boolean readOnly; // only allow statements that belong to QUERY category, fail for all other categories.
    private boolean rewrittenExpressionTree;
    private boolean logicalPlan;
    private boolean optimizedLogicalPlan;
    private boolean job;
    private boolean profile;
    private boolean signature;
    private boolean multiStatement;
    private long maxWarnings = DEFAULT_MAX_WARNINGS;

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

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isJob() {
        return job;
    }

    public void setJob(boolean job) {
        this.job = job;
    }

    public void setProfile(boolean profile) {
        this.profile = profile;
    }

    public boolean isProfile() {
        return profile;
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

    public void setMaxWarnings(long maxWarnings) {
        this.maxWarnings = maxWarnings;
    }

    public long getMaxWarnings() {
        return maxWarnings;
    }

    public ObjectNode asJson() {
        ObjectNode object = OBJECT_MAPPER.createObjectNode();
        object.put("host", host);
        object.put("path", path);
        object.put("statement", statement != null ? JSONUtil.escape(new StringBuilder(), statement).toString() : null);
        object.put("pretty", pretty);
        object.put("mode", mode);
        object.put("clientContextID", clientContextID);
        object.put("format", format);
        object.put("timeout", timeout);
        object.put("maxResultReads", maxResultReads);
        object.put("planFormat", planFormat);
        object.put("expressionTree", expressionTree);
        object.put("rewrittenExpressionTree", rewrittenExpressionTree);
        object.put("logicalPlan", logicalPlan);
        object.put("optimizedLogicalPlan", optimizedLogicalPlan);
        object.put("job", job);
        object.put("profile", profile);
        object.put("signature", signature);
        object.put("multiStatement", multiStatement);
        object.put("parseOnly", parseOnly);
        object.put("readOnly", readOnly);
        object.put("maxWarnings", maxWarnings);
        if (statementParams != null) {
            for (Map.Entry<String, JsonNode> statementParam : statementParams.entrySet()) {
                object.set('$' + statementParam.getKey(), statementParam.getValue());
            }
        }
        return object;
    }

    @Override
    public String toString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(asJson());
        } catch (JsonProcessingException e) {
            QueryServiceServlet.LOGGER.debug("unexpected exception marshalling {} instance to json", getClass(), e);
            return e.toString();
        }
    }
}
