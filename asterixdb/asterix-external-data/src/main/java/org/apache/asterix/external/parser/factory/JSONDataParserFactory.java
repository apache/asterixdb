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
package org.apache.asterix.external.parser.factory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

public class JSONDataParserFactory extends AbstractGenericDataParserFactory<char[]> {

    private static final long serialVersionUID = 2L;
    private static final List<String> PARSER_FORMAT = Collections.unmodifiableList(
            Arrays.asList(ExternalDataConstants.FORMAT_JSON_LOWER_CASE, ExternalDataConstants.FORMAT_JSON_UPPER_CASE));
    private static final JsonFactory jsonFactory;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        jsonFactory.configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, true);
        jsonFactory.configure(JsonFactory.Feature.INTERN_FIELD_NAMES, true);
    }

    @Override
    public IStreamDataParser createInputStreamParser(IExternalDataRuntimeContext context) {
        return createParser(context);
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        // no MetaType to set.
    }

    @Override
    public List<String> getParserFormats() {
        return PARSER_FORMAT;
    }

    @Override
    public IRecordDataParser<char[]> createRecordParser(IExternalDataRuntimeContext context) {
        return createParser(context);
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    private JSONDataParser createParser(IExternalDataRuntimeContext context) {
        return new JSONDataParser(recordType, jsonFactory, context);
    }

}
