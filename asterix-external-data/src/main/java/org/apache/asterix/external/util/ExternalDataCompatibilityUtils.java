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
import org.apache.asterix.om.types.ARecordType;

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

    //TODO:Add remaining aliases
    public static void addCompatabilityParameters(String adapterName, ARecordType itemType,
            Map<String, String> configuration) throws AsterixException {
        // HDFS
        if (adapterName.equals(ExternalDataConstants.ALIAS_HDFS_ADAPTER)
                || adapterName.equalsIgnoreCase(ExternalDataConstants.ADAPTER_HDFS_CLASSNAME)) {
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new AsterixException("Unspecified format parameter for HDFS adapter");
            }
            if (configuration.get(ExternalDataConstants.KEY_FORMAT).equals(ExternalDataConstants.FORMAT_BINARY)
                    || configuration.get(ExternalDataConstants.KEY_FORMAT).equals(ExternalDataConstants.FORMAT_HIVE)) {
                configuration.put(ExternalDataConstants.KEY_READER, ExternalDataConstants.READER_HDFS);
            } else {
                configuration.put(ExternalDataConstants.KEY_READER,
                        configuration.get(ExternalDataConstants.KEY_FORMAT));
                configuration.put(ExternalDataConstants.KEY_READER_STREAM, ExternalDataConstants.ALIAS_HDFS_ADAPTER);
            }
        }

        // Local Filesystem
        if (adapterName.equals(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER)
                || adapterName.contains(ExternalDataConstants.ADAPTER_LOCALFS_CLASSNAME)
                || adapterName.contains(ExternalDataConstants.ALIAS_LOCALFS_PUSH_ADAPTER)) {
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new AsterixException("Unspecified format parameter for local file system adapter");
            }
            configuration.put(ExternalDataConstants.KEY_READER, configuration.get(ExternalDataConstants.KEY_FORMAT));
            configuration.put(ExternalDataConstants.KEY_READER_STREAM, ExternalDataConstants.ALIAS_LOCALFS_ADAPTER);
        }

        // Twitter (Pull)
        if (adapterName.equals(ExternalDataConstants.ALIAS_TWITTER_PULL_ADAPTER)) {
            configuration.put(ExternalDataConstants.KEY_READER, ExternalDataConstants.READER_TWITTER_PULL);
            configuration.put(ExternalDataConstants.KEY_PULL, ExternalDataConstants.TRUE);
            ExternalDataUtils.setRecordFormat(configuration, ExternalDataConstants.FORMAT_TWEET);
        }

        // Twitter (Push)
        if (adapterName.equals(ExternalDataConstants.ALIAS_TWITTER_PUSH_ADAPTER)) {
            configuration.put(ExternalDataConstants.KEY_READER, ExternalDataConstants.READER_TWITTER_PUSH);
            configuration.put(ExternalDataConstants.KEY_PUSH, ExternalDataConstants.TRUE);
            ExternalDataUtils.setRecordFormat(configuration, ExternalDataConstants.FORMAT_TWEET);
        }

        // Hive Parser
        if (configuration.get(ExternalDataConstants.KEY_PARSER) != null
                && configuration.get(ExternalDataConstants.KEY_PARSER).equals(ExternalDataConstants.PARSER_HIVE)) {
            configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_HIVE);
        }

        // FileSystem for Feed adapter
        if (configuration.get(ExternalDataConstants.KEY_FILESYSTEM) != null) {
            configuration.put(ExternalDataConstants.KEY_STREAM,
                    configuration.get(ExternalDataConstants.KEY_FILESYSTEM));
            if (adapterName.equalsIgnoreCase(ExternalDataConstants.ALIAS_FILE_FEED_ADAPTER)) {
                configuration.put(ExternalDataConstants.KEY_WAIT_FOR_DATA, ExternalDataConstants.FALSE);
            }
        }
    }
}
