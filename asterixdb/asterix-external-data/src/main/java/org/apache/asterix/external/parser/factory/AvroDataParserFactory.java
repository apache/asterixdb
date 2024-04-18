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

import java.util.List;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.AvroDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.avro.generic.GenericRecord;

public class AvroDataParserFactory extends AbstractGenericDataParserFactory<GenericRecord> {

    private static final long serialVersionUID = 1L;
    private static final List<String> PARSER_FORMAT = List.of(ExternalDataConstants.FORMAT_AVRO);

    @Override
    public IStreamDataParser createInputStreamParser(IExternalDataRuntimeContext context) {
        throw new UnsupportedOperationException("Stream parser is not supported");
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
    public IRecordDataParser<GenericRecord> createRecordParser(IExternalDataRuntimeContext context) {
        return createParser(context);
    }

    @Override
    public Class<?> getRecordClass() {
        return GenericRecord.class;
    }

    private AvroDataParser createParser(IExternalDataRuntimeContext context) {
        return new AvroDataParser(context);
    }

}
