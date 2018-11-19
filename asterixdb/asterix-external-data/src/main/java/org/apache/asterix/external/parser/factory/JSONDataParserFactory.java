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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

public class JSONDataParserFactory extends AbstractRecordStreamParserFactory<char[]> {

    private static final long serialVersionUID = 1L;
    private static final List<String> PARSER_FORMAT = Collections.unmodifiableList(
            Arrays.asList(ExternalDataConstants.FORMAT_JSON_LOWER_CASE, ExternalDataConstants.FORMAT_JSON_UPPER_CASE));
    private static final List<ATypeTag> UNSUPPORTED_TYPES = Collections
            .unmodifiableList(Arrays.asList(ATypeTag.MULTISET, ATypeTag.POINT3D, ATypeTag.CIRCLE, ATypeTag.RECTANGLE,
                    ATypeTag.INTERVAL, ATypeTag.DAYTIMEDURATION, ATypeTag.DURATION, ATypeTag.BINARY));

    private final JsonFactory jsonFactory;

    public JSONDataParserFactory() {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        jsonFactory.configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, true);
        jsonFactory.configure(JsonFactory.Feature.INTERN_FIELD_NAMES, true);
    }

    @Override
    public IStreamDataParser createInputStreamParser(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return createParser();
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
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return createParser();
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    private JSONDataParser createParser() throws HyracksDataException {
        return new JSONDataParser(recordType, jsonFactory);
    }

    /*
     * check type compatibility before creating the parser.
     */
    @Override
    public void setRecordType(ARecordType recordType) throws AsterixException {
        checkRecordTypeCompatibility(recordType);
        super.setRecordType(recordType);
    }

    /**
     * Check if the defined type contains ADM special types.
     * if it contains unsupported types.
     *
     * @param recordType
     * @throws AsterixException
     */
    private void checkRecordTypeCompatibility(ARecordType recordType) throws AsterixException {
        final IAType[] fieldTypes = recordType.getFieldTypes();
        for (IAType type : fieldTypes) {
            checkTypeCompatibility(type);
        }
    }

    private void checkTypeCompatibility(IAType type) throws AsterixException {
        if (UNSUPPORTED_TYPES.contains(type.getTypeTag())) {
            throw new AsterixException(ErrorCode.TYPE_UNSUPPORTED, JSONDataParserFactory.class.getName(),
                    type.getTypeTag().toString());
        } else if (type.getTypeTag() == ATypeTag.ARRAY) {
            checkTypeCompatibility(((AOrderedListType) type).getItemType());
        } else if (type.getTypeTag() == ATypeTag.OBJECT) {
            checkRecordTypeCompatibility((ARecordType) type);
        } else if (type.getTypeTag() == ATypeTag.UNION) {
            checkTypeCompatibility(((AUnionType) type).getActualType());
        }
        //Compatible type
    }

}
