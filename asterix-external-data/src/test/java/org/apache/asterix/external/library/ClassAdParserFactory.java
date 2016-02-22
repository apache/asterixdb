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
package org.apache.asterix.external.library;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClassAdParserFactory implements IRecordDataParserFactory<char[]> {

    private static final long serialVersionUID = 1L;
    public static final String KEY_OLD_FORMAT = "old-format";
    public static final String KEY_EVALUATE = "evaluate";
    public static final String KEY_KEEP_EXPR = "keep-expr";
    public static final String KEY_EXPR_PREFIX = "expr-prefix";
    public static final String KEY_EXPR_SUFFIX = "expr-suffix";
    public static final String KEY_EXPR_NAME_SUFFIX = "expr-name-suffix";

    private ARecordType recordType;
    private Map<String, String> configuration;
    private boolean oldFormat = false;

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
        stream.writeObject(recordType);
        stream.writeObject(configuration);
    }

    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
        recordType = (ARecordType) stream.readObject();
        configuration = (Map<String, String>) stream.readObject();
    }

    @Override
    public DataSourceType getDataSourceType() throws AsterixException {
        return DataSourceType.RECORDS;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        // is old format?
        String parserConfig = configuration.get(KEY_OLD_FORMAT);
        if (parserConfig != null && parserConfig.equalsIgnoreCase(ExternalDataConstants.TRUE)) {
            oldFormat = true;
        }
        parserConfig = configuration.get(ExternalDataConstants.KEY_READER);
        if (parserConfig != null && parserConfig.equalsIgnoreCase(ExternalDataConstants.READER_LINE_SEPARATED)) {
            oldFormat = true;
        }
        if (!oldFormat) {
            configuration.put(ExternalDataConstants.KEY_RECORD_START, "[");
            configuration.put(ExternalDataConstants.KEY_RECORD_END, "]");
        }

    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx)
            throws HyracksDataException, AsterixException, IOException {
        ClassAdParser parser = new ClassAdParser(recordType);
        parser.configure(configuration, recordType);
        return parser;
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

}
