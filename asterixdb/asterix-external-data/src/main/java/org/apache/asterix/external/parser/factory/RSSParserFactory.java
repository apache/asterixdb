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
import org.apache.asterix.external.parser.RSSParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

import com.rometools.rome.feed.synd.SyndEntry;

public class RSSParserFactory implements IRecordDataParserFactory<SyndEntry> {

    private static final long serialVersionUID = 1L;
    private static final List<String> parserFormats = Collections.unmodifiableList(Arrays.asList("rss"));
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
    public IRecordDataParser<SyndEntry> createRecordParser(IHyracksTaskContext ctx) {
        RSSParser dataParser = new RSSParser(recordType);
        return dataParser;
    }

    @Override
    public Class<? extends SyndEntry> getRecordClass() {
        return SyndEntry.class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }

}
