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
package org.apache.asterix.external.parser;

import static org.apache.asterix.common.exceptions.ErrorCode.PARSER_DATA_PARSER_UNEXPECTED_TOKEN;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.jackson.ADMToken;
import org.apache.asterix.external.provider.context.NoOpExternalRuntimeDataContext;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;

/**
 * JSON format parser using Jackson parser.
 */
public class JSONDataParser extends AbstractJsonDataParser implements IStreamDataParser, IRecordDataParser<char[]> {

    /**
     * This constructor is used by several query runtime evaluators and tests
     *
     * @param recordType  defined type.
     * @param jsonFactory Jackson JSON parser factory.
     */
    public JSONDataParser(ARecordType recordType, JsonFactory jsonFactory) {
        this(recordType, jsonFactory, NoOpExternalRuntimeDataContext.INSTANCE);
    }

    /**
     * Initialize JSONDataParser
     *
     * @param recordType  defined type.
     * @param jsonFactory Jackson JSON parser factory.
     */
    public JSONDataParser(ARecordType recordType, JsonFactory jsonFactory, IExternalDataRuntimeContext context) {
        super(recordType, jsonFactory, context);
    }

    @Override
    public void setInputStream(InputStream in) throws IOException {
        setInput(jsonFactory.createParser(in));
    }

    public void setInputNode(JsonNode node) {
        setInput(new TreeTraversingParser(node));
    }

    private void setInput(JsonParser parser) {
        jsonParser = parser;
        geometryCoParser.reset(jsonParser);
    }

    @Override
    public boolean parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO(wyk): find a way to reset byte[] instead of creating a new parser for each record.
            jsonParser = jsonFactory.createParser(record.get(), 0, record.size());
            geometryCoParser.reset(jsonParser);
            if (nextToken() != ADMToken.OBJECT_START) {
                throw new ParseException(PARSER_DATA_PARSER_UNEXPECTED_TOKEN, currentToken(), ADMToken.OBJECT_START);
            }
            valueEmbedder.reset();
            parseObject(rootType, out);
            return true;
        } catch (IOException e) {
            throw createException(e);
        }
    }

    @Override
    public boolean parse(DataOutput out) throws HyracksDataException {
        try {
            if (nextToken() == ADMToken.EOF) {
                return false;
            }
            valueEmbedder.reset();
            parseObject(rootType, out);
            return true;
        } catch (IOException e) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM, e);
        }
    }

    @Override
    public boolean reset(InputStream in) throws IOException {
        setInputStream(in);
        return true;
    }
}
