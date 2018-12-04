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
import java.util.Map;

import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.TweetParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class TweetParserFactory implements IRecordDataParserFactory<char[]> {

    private static final long serialVersionUID = 1L;
    private static final List<String> parserFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_TWEET));
    private ARecordType recordType;

    @Override
    public void configure(Map<String, String> configuration) {
        // Nothing to be configured.
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx) {
        TweetParser dataParser = new TweetParser(recordType);
        return dataParser;
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        // do nothing
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }

}
