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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.NoOpDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class NoOpDataParserFactory implements IRecordDataParserFactory<IValueReference> {
    private static final long serialVersionUID = 5274870143009767516L;

    private static final List<String> PARSER_FORMAT = Collections.singletonList(ExternalDataConstants.FORMAT_NOOP);

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException {
        //nothing
    }

    @Override
    public void setRecordType(ARecordType recordType) throws AsterixException {
        //it always return open type
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        //no meta type
    }

    @Override
    public List<String> getParserFormats() {
        return PARSER_FORMAT;
    }

    @Override
    public IRecordDataParser<IValueReference> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new NoOpDataParser();
    }

    @Override
    public Class<?> getRecordClass() {
        return IValueReference.class;
    }

}
