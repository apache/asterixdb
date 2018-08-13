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

import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.HiveRecordParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hadoop.io.Writable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class HiveDataParserFactory implements IRecordDataParserFactory<Writable> {

    private static final long serialVersionUID = 1L;
    private static final List<String> parserFormats =
            Collections.unmodifiableList(Arrays.asList("hive", "hive-parser"));
    private Map<String, String> configuration;
    private ARecordType recordType;
    private String hiveSerdeClassName;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public void configure(Map<String, String> configuration) {
        this.configuration = configuration;
        hiveSerdeClassName = configuration.get(ExternalDataConstants.KEY_HIVE_SERDE);
        if (hiveSerdeClassName == null) {
            throw new IllegalArgumentException("no hive serde provided for hive deserialized records");
        }
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public IRecordDataParser<Writable> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new HiveRecordParser(recordType, HDFSUtils.configureHDFSJobConf(configuration), hiveSerdeClassName);
    }

    @Override
    public Class<? extends Writable> getRecordClass() {
        return Writable.class;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }
}
