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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.input.record.RecordWithPK;
import org.apache.asterix.external.parser.TestRecordWithPKParser;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings({ "unchecked" })
public class TestRecordWithPKParserFactory<T> implements IRecordDataParserFactory<RecordWithPK<T>> {

    private static final long serialVersionUID = 1L;
    private static final ArrayList<String> parserFormats = new ArrayList<>();
    private ARecordType recordType;
    private IRecordDataParserFactory<char[]> recordParserFactory;
    private String format;
    @SuppressWarnings("unused")
    private IAType[] pkTypes;
    @SuppressWarnings("unused")
    private int[][] pkIndexes;

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException {
        TreeMap<String, String> parserConf = new TreeMap<String, String>();
        format = configuration.get(ExternalDataConstants.KEY_RECORD_FORMAT);
        parserFormats.add(format);
        parserConf.put(ExternalDataConstants.KEY_FORMAT, format);
        recordParserFactory =
                (IRecordDataParserFactory<char[]>) ParserFactoryProvider.getDataParserFactory(null, parserConf);
        recordParserFactory.setRecordType(recordType);
        recordParserFactory.configure(configuration);
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IRecordDataParser<RecordWithPK<T>> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new TestRecordWithPKParser(recordParserFactory.createRecordParser(ctx));
    }

    @Override
    public Class<?> getRecordClass() {
        return RecordWithPK.class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }

}
