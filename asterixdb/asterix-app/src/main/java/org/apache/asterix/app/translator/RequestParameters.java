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
package org.apache.asterix.app.translator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.ResultProperties;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

import com.fasterxml.jackson.databind.JsonNode;

public class RequestParameters implements IRequestParameters {

    public static final int NO_CATEGORY_RESTRICTION_MASK = 0;

    private final IRequestReference requestReference;
    private final IResultSet resultSet;
    private final ResultProperties resultProperties;
    private final Stats stats;
    private final Map<String, String> optionalParameters;
    private final IStatementExecutor.ResultMetadata outMetadata;
    private final String clientContextId;
    private final Map<String, IAObject> statementParameters;
    private final boolean multiStatement;
    private final int statementCategoryRestrictionMask;
    private final String statement;

    public RequestParameters(IRequestReference requestReference, String statement, IResultSet resultSet,
            ResultProperties resultProperties, Stats stats, IStatementExecutor.ResultMetadata outMetadata,
            String clientContextId, Map<String, String> optionalParameters, Map<String, IAObject> statementParameters,
            boolean multiStatement) {
        this(requestReference, statement, resultSet, resultProperties, stats, outMetadata, clientContextId,
                optionalParameters, statementParameters, multiStatement, NO_CATEGORY_RESTRICTION_MASK);
    }

    public RequestParameters(IRequestReference requestReference, String statement, IResultSet resultSet,
            ResultProperties resultProperties, Stats stats, IStatementExecutor.ResultMetadata outMetadata,
            String clientContextId, Map<String, String> optionalParameters, Map<String, IAObject> statementParameters,
            boolean multiStatement, int statementCategoryRestrictionMask) {
        this.requestReference = requestReference;
        this.statement = statement;
        this.resultSet = resultSet;
        this.resultProperties = resultProperties;
        this.stats = stats;
        this.outMetadata = outMetadata;
        this.clientContextId = clientContextId;
        this.optionalParameters = optionalParameters;
        this.statementParameters = statementParameters;
        this.multiStatement = multiStatement;
        this.statementCategoryRestrictionMask = statementCategoryRestrictionMask;
    }

    @Override
    public IResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public ResultProperties getResultProperties() {
        return resultProperties;
    }

    @Override
    public IStatementExecutor.Stats getStats() {
        return stats;
    }

    @Override
    public Map<String, String> getOptionalParameters() {
        return optionalParameters;
    }

    @Override
    public IStatementExecutor.ResultMetadata getOutMetadata() {
        return outMetadata;
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
    }

    @Override
    public boolean isMultiStatement() {
        return multiStatement;
    }

    @Override
    public int getStatementCategoryRestrictionMask() {
        return statementCategoryRestrictionMask;
    }

    @Override
    public Map<String, IAObject> getStatementParameters() {
        return statementParameters;
    }

    @Override
    public String getStatement() {
        return statement;
    }

    @Override
    public IRequestReference getRequestReference() {
        return requestReference;
    }

    public static Map<String, byte[]> serializeParameterValues(Map<String, JsonNode> inParams)
            throws HyracksDataException {
        if (inParams == null || inParams.isEmpty()) {
            return null;
        }
        JSONDataParser parser = new JSONDataParser(null, null);
        ByteArrayAccessibleOutputStream buffer = new ByteArrayAccessibleOutputStream();
        DataOutputStream bufferDataOutput = new DataOutputStream(buffer);
        Map<String, byte[]> m = new HashMap<>();
        for (Map.Entry<String, JsonNode> me : inParams.entrySet()) {
            String name = me.getKey();
            JsonNode jsonValue = me.getValue();
            parser.setInputNode(jsonValue);
            buffer.reset();
            parser.parseAnyValue(bufferDataOutput);
            byte[] byteValue = buffer.toByteArray();
            m.put(name, byteValue);
        }
        return m;
    }

    public static Map<String, IAObject> deserializeParameterValues(Map<String, byte[]> inParams)
            throws HyracksDataException {
        if (inParams == null || inParams.isEmpty()) {
            return null;
        }
        Map<String, IAObject> m = new HashMap<>();
        ByteArrayAccessibleInputStream buffer = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
        DataInputStream bufferDataInput = new DataInputStream(buffer);
        ISerializerDeserializer serDe =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANY);
        for (Map.Entry<String, byte[]> me : inParams.entrySet()) {
            String name = me.getKey();
            byte[] value = me.getValue();
            buffer.setContent(value, 0, value.length);
            IAObject iaValue = (IAObject) serDe.deserialize(bufferDataInput);
            m.put(name, iaValue);
        }
        return m;
    }

    public static int getStatementCategoryRestrictionMask(boolean readOnly) {
        return readOnly ? Statement.Category.QUERY : NO_CATEGORY_RESTRICTION_MASK;
    }
}
