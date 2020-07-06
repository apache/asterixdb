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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.asterix.common.api.Duration;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats.ProfileType;
import org.apache.asterix.translator.SessionConfig.OutputFormat;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHeaders;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

public class QueryServiceRequestParameters {

    public enum Parameter {
        ARGS("args"),
        STATEMENT("statement"),
        FORMAT("format"),
        CLIENT_ID("client_context_id"),
        PRETTY("pretty"),
        MODE("mode"),
        TIMEOUT("timeout"),
        PLAN_FORMAT("plan-format"),
        MAX_RESULT_READS("max-result-reads"),
        EXPRESSION_TREE("expression-tree"),
        REWRITTEN_EXPRESSION_TREE("rewritten-expression-tree"),
        LOGICAL_PLAN("logical-plan"),
        OPTIMIZED_LOGICAL_PLAN("optimized-logical-plan"),
        PARSE_ONLY("parse-only"),
        READ_ONLY("readonly"),
        JOB("job"),
        PROFILE("profile"),
        SIGNATURE("signature"),
        MULTI_STATEMENT("multi-statement"),
        MAX_WARNINGS("max-warnings");

        private final String str;

        Parameter(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private enum Attribute {
        HEADER("header"),
        LOSSLESS("lossless");

        private final String str;

        Attribute(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private static final Map<String, PlanFormat> planFormats = ImmutableMap.of(HttpUtil.ContentType.JSON,
            PlanFormat.JSON, "clean_json", PlanFormat.JSON, "string", PlanFormat.STRING);
    private static final Map<String, Boolean> booleanValues =
            ImmutableMap.of(Boolean.TRUE.toString(), Boolean.TRUE, Boolean.FALSE.toString(), Boolean.FALSE);
    private static final Map<String, Boolean> csvHeaderValues =
            ImmutableMap.of("present", Boolean.TRUE, "absent", Boolean.FALSE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String host;
    private String path;
    private String statement;
    private String clientContextID;
    private OutputFormat format = OutputFormat.CLEAN_JSON;
    private ResultDelivery mode = ResultDelivery.IMMEDIATE;
    private PlanFormat planFormat = PlanFormat.JSON;
    private ProfileType profileType = ProfileType.COUNTS;
    private Map<String, String> optionalParams = null;
    private Map<String, JsonNode> statementParams = null;
    private boolean pretty = false;
    private boolean expressionTree = false;
    private boolean parseOnly = false; // don't execute; simply check for syntax correctness and named parameters.
    private boolean readOnly = false; // only allow statements belonging to QUERY category, fail for other categories.
    private boolean rewrittenExpressionTree = false;
    private boolean logicalPlan = false;
    private boolean optimizedLogicalPlan = false;
    private boolean job = false;
    private boolean isCSVWithHeader = false;
    private boolean signature = true;
    private boolean multiStatement = true;
    private long timeout = TimeUnit.MILLISECONDS.toMillis(Long.MAX_VALUE);
    private long maxResultReads = 1L;
    private long maxWarnings = 0L;

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

    public OutputFormat getFormat() {
        return format;
    }

    public void setFormat(Pair<OutputFormat, Boolean> formatAndHeader) {
        Objects.requireNonNull(formatAndHeader);
        Objects.requireNonNull(formatAndHeader.getLeft());
        this.format = formatAndHeader.getLeft();
        if (format == OutputFormat.CSV) {
            isCSVWithHeader = formatAndHeader.getRight();
        }
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
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

    public ResultDelivery getMode() {
        return mode;
    }

    public void setMode(ResultDelivery mode) {
        Objects.requireNonNull(mode);
        this.mode = mode;
    }

    public long getMaxResultReads() {
        return maxResultReads;
    }

    public void setMaxResultReads(long maxResultReads) {
        this.maxResultReads = maxResultReads;
    }

    public PlanFormat getPlanFormat() {
        return planFormat;
    }

    public void setPlanFormat(PlanFormat planFormat) {
        Objects.requireNonNull(planFormat);
        this.planFormat = planFormat;
    }

    public Map<String, String> getOptionalParams() {
        return optionalParams;
    }

    public void setOptionalParams(Map<String, String> optionalParams) {
        this.optionalParams = optionalParams;
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

    public void setProfileType(ProfileType profileType) {
        Objects.requireNonNull(profileType);
        this.profileType = profileType;
    }

    public ProfileType getProfileType() {
        return profileType;
    }

    public boolean isCSVWithHeader() {
        return format == OutputFormat.CSV && isCSVWithHeader;
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
        object.put("mode", mode.getName());
        object.put("clientContextID", clientContextID);
        object.put("format", format.toString());
        object.put("timeout", timeout);
        object.put("maxResultReads", maxResultReads);
        object.put("planFormat", planFormat.toString());
        object.put("expressionTree", expressionTree);
        object.put("rewrittenExpressionTree", rewrittenExpressionTree);
        object.put("logicalPlan", logicalPlan);
        object.put("optimizedLogicalPlan", optimizedLogicalPlan);
        object.put("job", job);
        object.put("profile", profileType.getName());
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

    public void setParameters(QueryServiceServlet servlet, IServletRequest request, Map<String, String> optionalParams)
            throws IOException {
        setHost(servlet.host(request));
        setPath(servlet.servletPath(request));
        String contentType = HttpUtil.getContentTypeOnly(request);
        setOptionalParams(optionalParams);
        try {
            if (HttpUtil.ContentType.APPLICATION_JSON.equals(contentType)) {
                setParamFromJSON(request, optionalParams);
            } else {
                setParamFromRequest(request, optionalParams);
            }
        } catch (JsonParseException | JsonMappingException e) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_JSON_VAL);
        }
    }

    private void setParamFromJSON(IServletRequest request, Map<String, String> optionalParameters) throws IOException {
        JsonNode jsonRequest = OBJECT_MAPPER.readTree(HttpUtil.getRequestBody(request));
        setParams(jsonRequest, request.getHeader(HttpHeaders.ACCEPT), QueryServiceRequestParameters::getParameter);
        setStatementParams(getOptStatementParameters(jsonRequest, jsonRequest.fieldNames(), JsonNode::get, v -> v));
        setJsonOptionalParameters(jsonRequest, optionalParameters);
    }

    private void setParamFromRequest(IServletRequest request, Map<String, String> optionalParameters)
            throws IOException {
        setParams(request, request.getHeader(HttpHeaders.ACCEPT), IServletRequest::getParameter);
        setStatementParams(getOptStatementParameters(request, request.getParameterNames().iterator(),
                IServletRequest::getParameter, OBJECT_MAPPER::readTree));
        setOptionalParameters(request, optionalParameters);
    }

    private <Req> void setParams(Req req, String acceptHeader, BiFunction<Req, String, String> valGetter)
            throws HyracksDataException {
        setStatement(valGetter.apply(req, Parameter.STATEMENT.str()));
        setClientContextID(valGetter.apply(req, Parameter.CLIENT_ID.str()));

        setFormatIfExists(req, acceptHeader, Parameter.FORMAT.str(), valGetter);
        setMode(parseIfExists(req, Parameter.MODE.str(), valGetter, getMode(), ResultDelivery::fromName));
        setPlanFormat(parseIfExists(req, Parameter.PLAN_FORMAT.str(), valGetter, getPlanFormat(), planFormats::get));
        setProfileType(parseIfExists(req, Parameter.PROFILE.str(), valGetter, getProfileType(), ProfileType::fromName));

        setTimeout(parseTime(req, Parameter.TIMEOUT.str(), valGetter, getTimeout()));
        setMaxResultReads(parseLong(req, Parameter.MAX_RESULT_READS.str(), valGetter, getMaxResultReads()));
        setMaxWarnings(parseLong(req, Parameter.MAX_WARNINGS.str(), valGetter, getMaxWarnings()));

        setPretty(parseBoolean(req, Parameter.PRETTY.str(), valGetter, isPretty()));
        setExpressionTree(parseBoolean(req, Parameter.EXPRESSION_TREE.str(), valGetter, isExpressionTree()));
        setRewrittenExpressionTree(
                parseBoolean(req, Parameter.REWRITTEN_EXPRESSION_TREE.str(), valGetter, isRewrittenExpressionTree()));
        setLogicalPlan(parseBoolean(req, Parameter.LOGICAL_PLAN.str(), valGetter, isLogicalPlan()));
        setParseOnly(parseBoolean(req, Parameter.PARSE_ONLY.str(), valGetter, isParseOnly()));
        setReadOnly(parseBoolean(req, Parameter.READ_ONLY.str(), valGetter, isReadOnly()));
        setOptimizedLogicalPlan(
                parseBoolean(req, Parameter.OPTIMIZED_LOGICAL_PLAN.str(), valGetter, isOptimizedLogicalPlan()));
        setMultiStatement(parseBoolean(req, Parameter.MULTI_STATEMENT.str(), valGetter, isMultiStatement()));
        setJob(parseBoolean(req, Parameter.JOB.str(), valGetter, isJob()));
        setSignature(parseBoolean(req, Parameter.SIGNATURE.str(), valGetter, isSignature()));
    }

    protected void setJsonOptionalParameters(JsonNode jsonRequest, Map<String, String> optionalParameters)
            throws HyracksDataException {
        // allows extensions to set extra parameters
    }

    protected void setOptionalParameters(IServletRequest request, Map<String, String> optionalParameters)
            throws HyracksDataException {
        // allows extensions to set extra parameters
    }

    @FunctionalInterface
    interface CheckedFunction<I, O> {
        O apply(I requestParamValue) throws IOException;
    }

    private <R, P> Map<String, JsonNode> getOptStatementParameters(R request, Iterator<String> paramNameIter,
            BiFunction<R, String, P> paramValueAccessor, CheckedFunction<P, JsonNode> paramValueParser)
            throws IOException {
        Map<String, JsonNode> result = null;
        while (paramNameIter.hasNext()) {
            String paramName = paramNameIter.next();
            String stmtParamName = extractStatementParameterName(paramName);
            if (stmtParamName != null) {
                if (result == null) {
                    result = new HashMap<>();
                }
                P paramValue = paramValueAccessor.apply(request, paramName);
                JsonNode stmtParamValue = paramValueParser.apply(paramValue);
                result.put(stmtParamName, stmtParamValue);
            } else if (Parameter.ARGS.str().equals(toLower(paramName))) {
                if (result == null) {
                    result = new HashMap<>();
                }
                P paramValue = paramValueAccessor.apply(request, paramName);
                JsonNode stmtParamValue = paramValueParser.apply(paramValue);
                if (!stmtParamValue.isArray()) {
                    throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, paramName, stmtParamValue.asText());
                }
                for (int i = 0, ln = stmtParamValue.size(); i < ln; i++) {
                    result.put(String.valueOf(i + 1), stmtParamValue.get(i));
                }
            }
        }
        return result;
    }

    public static String extractStatementParameterName(String name) {
        int ln = name.length();
        if ((ln == 2 || isStatementParameterNameRest(name, 2)) && name.charAt(0) == '$'
                && Character.isLetter(name.charAt(1))) {
            return name.substring(1);
        }
        return null;
    }

    private static boolean isStatementParameterNameRest(CharSequence input, int startIndex) {
        int i = startIndex;
        for (int ln = input.length(); i < ln; i++) {
            char c = input.charAt(i);
            boolean ok = c == '_' || Character.isLetterOrDigit(c);
            if (!ok) {
                return false;
            }
        }
        return i > startIndex;
    }

    private static <R> boolean parseBoolean(R request, String parameterName,
            BiFunction<R, String, String> valueAccessor, boolean defaultVal) throws HyracksDataException {
        String value = toLower(valueAccessor.apply(request, parameterName));
        if (value == null) {
            return defaultVal;
        }
        Boolean booleanVal = booleanValues.get(value);
        if (booleanVal == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, parameterName, value);

        }
        return booleanVal.booleanValue();
    }

    private static <R> long parseLong(R request, String parameterName, BiFunction<R, String, String> valueAccessor,
            long defaultVal) throws HyracksDataException {
        String value = toLower(valueAccessor.apply(request, parameterName));
        if (value == null) {
            return defaultVal;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, parameterName, value);
        }
    }

    private static <R> long parseTime(R request, String parameterName, BiFunction<R, String, String> valueAccessor,
            long def) throws HyracksDataException {
        String value = toLower(valueAccessor.apply(request, parameterName));
        if (value == null) {
            return def;
        }
        try {
            return TimeUnit.NANOSECONDS.toMillis(Duration.parseDurationStringToNanos(value));
        } catch (HyracksDataException e) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, parameterName, value);
        }
    }

    private <R> void setFormatIfExists(R request, String acceptHeader, String parameterName,
            BiFunction<R, String, String> valueAccessor) throws HyracksDataException {
        Pair<OutputFormat, Boolean> formatAndHeader =
                parseFormatIfExists(request, acceptHeader, parameterName, valueAccessor);
        if (formatAndHeader != null) {
            setFormat(formatAndHeader);
        }
    }

    protected <R> Pair<OutputFormat, Boolean> parseFormatIfExists(R request, String acceptHeader, String parameterName,
            BiFunction<R, String, String> valueAccessor) throws HyracksDataException {
        String value = toLower(valueAccessor.apply(request, parameterName));
        if (value == null) {
            // if no value is provided in request parameter "format", then check "Accept" parameter in HEADER
            // and only validate attribute val if mime and attribute name are known, e.g. application/json;lossless=?
            if (acceptHeader != null) {
                String[] mimeTypes = StringUtils.split(acceptHeader, ',');
                for (int i = 0, size = mimeTypes.length; i < size; i++) {
                    Pair<OutputFormat, Boolean> formatAndHeader = fromMime(mimeTypes[i]);
                    if (formatAndHeader != null) {
                        return formatAndHeader;
                    }
                }
            }
            return null;
        }
        // checking value in request parameter "format"
        if (value.equals(HttpUtil.ContentType.CSV)) {
            return Pair.of(OutputFormat.CSV, Boolean.FALSE);
        } else if (value.equals(HttpUtil.ContentType.JSON)) {
            return Pair.of(OutputFormat.CLEAN_JSON, Boolean.FALSE);
        } else if (value.equals(HttpUtil.ContentType.ADM)) {
            return Pair.of(OutputFormat.ADM, Boolean.FALSE);
        } else {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, parameterName, value);
        }
    }

    private static Pair<OutputFormat, Boolean> fromMime(String mimeType) throws HyracksDataException {
        // find the first match, no preferences for now
        String[] mimeSplits = StringUtils.split(mimeType, ';');
        if (mimeSplits.length > 0) {
            String format = mimeSplits[0].toLowerCase().trim();
            if (format.equals(HttpUtil.ContentType.APPLICATION_JSON)) {
                return Pair.of(hasValue(mimeSplits, Attribute.LOSSLESS.str(), booleanValues)
                        ? OutputFormat.LOSSLESS_JSON : OutputFormat.CLEAN_JSON, Boolean.FALSE);
            } else if (format.equals(HttpUtil.ContentType.TEXT_CSV)) {
                return Pair.of(OutputFormat.CSV,
                        hasValue(mimeSplits, Attribute.HEADER.str(), csvHeaderValues) ? Boolean.TRUE : Boolean.FALSE);
            } else if (format.equals(HttpUtil.ContentType.APPLICATION_ADM)) {
                return Pair.of(OutputFormat.ADM, Boolean.FALSE);
            }
        }
        return null;
    }

    private static boolean hasValue(String[] mimeTypeParts, String attributeName, Map<String, Boolean> allowedValues)
            throws HyracksDataException {
        for (int i = 1, size = mimeTypeParts.length; i < size; i++) {
            String[] attNameAndVal = StringUtils.split(mimeTypeParts[i], '=');
            if (attNameAndVal.length == 2 && attNameAndVal[0].toLowerCase().trim().equals(attributeName)) {
                Boolean value = allowedValues.get(attNameAndVal[1].toLowerCase().trim());
                if (value == null) {
                    throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, attributeName, attNameAndVal[1]);
                }
                return value.booleanValue();
            }
        }
        return false;
    }

    private static <Req, Param> Param parseIfExists(Req request, String parameterName,
            BiFunction<Req, String, String> valueAccessor, Param defaultVal, Function<String, Param> parseFunction)
            throws HyracksDataException {
        String valueInRequest = toLower(valueAccessor.apply(request, parameterName));
        if (valueInRequest == null) {
            return defaultVal;
        }
        Param resultValue = parseFunction.apply(valueInRequest);
        if (resultValue == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, parameterName, valueInRequest);
        }
        return resultValue;
    }

    protected static String getParameter(JsonNode node, String parameter) {
        final JsonNode value = node.get(parameter);
        return value != null ? value.asText() : null;
    }

    protected static String toLower(String s) {
        return s != null ? s.toLowerCase() : s;
    }
}
