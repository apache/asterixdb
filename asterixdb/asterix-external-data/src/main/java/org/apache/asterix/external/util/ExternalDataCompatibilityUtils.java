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
package org.apache.asterix.external.util;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.converter.IRecordConverterFactory;

public class ExternalDataCompatibilityUtils {

    public static void validateCompatibility(IExternalDataSourceFactory dataSourceFactory,
            IDataParserFactory dataParserFactory) throws AsterixException {
        if (dataSourceFactory.getDataSourceType() != dataParserFactory.getDataSourceType()) {
            throw new AsterixException(
                    "datasource-parser mismatch. datasource produces " + dataSourceFactory.getDataSourceType()
                            + " and parser expects " + dataParserFactory.getDataSourceType());
        }
        if (dataSourceFactory.getDataSourceType() == DataSourceType.RECORDS) {
            IRecordReaderFactory<?> recordReaderFactory = (IRecordReaderFactory<?>) dataSourceFactory;
            IRecordDataParserFactory<?> recordParserFactory = (IRecordDataParserFactory<?>) dataParserFactory;
            if (!recordParserFactory.getRecordClass().isAssignableFrom(recordReaderFactory.getRecordClass())) {
                throw new AsterixException("datasource-parser mismatch. datasource produces records of type "
                        + recordReaderFactory.getRecordClass() + " and parser expects records of type "
                        + recordParserFactory.getRecordClass());
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void validateCompatibility(IRecordDataParserFactory recordParserFactory,
            IRecordConverterFactory converterFactory) throws AsterixException {
        if (!recordParserFactory.getRecordClass().isAssignableFrom(converterFactory.getOutputClass())) {
            throw new AsterixException(
                    "datasource converter-record parser mismatch. converter produces records of type "
                            + converterFactory.getOutputClass() + " and parser expects records of type "
                            + recordParserFactory.getRecordClass());
        }
    }

    public static void prepare(String adapterName, Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_READER)) {
            configuration.put(ExternalDataConstants.KEY_READER, adapterName);
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_PARSER)) {
            if (configuration.containsKey(ExternalDataConstants.KEY_FORMAT)) {
                configuration.put(ExternalDataConstants.KEY_PARSER,
                        configuration.get(ExternalDataConstants.KEY_FORMAT));
            }
        }
    }
}
