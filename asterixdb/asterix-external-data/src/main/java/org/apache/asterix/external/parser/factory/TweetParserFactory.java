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

import java.util.Map;

import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.TweetParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

import twitter4j.Status;

public class TweetParserFactory implements IRecordDataParserFactory<String> {

    private static final long serialVersionUID = 1L;
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
    public IRecordDataParser<String> createRecordParser(IHyracksTaskContext ctx) {
        TweetParser dataParser = new TweetParser(recordType);
        return dataParser;
    }

    @Override
    public Class<? extends String> getRecordClass() {
        return String.class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        // do nothing
    }

}
