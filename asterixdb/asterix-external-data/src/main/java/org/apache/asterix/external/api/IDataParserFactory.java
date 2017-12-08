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
package org.apache.asterix.external.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public interface IDataParserFactory extends Serializable {

    /**
     * @return The expected data source type {STREAM or RECORDS}
     *         The data source type for a parser and a data source must match.
     *         an instance of IDataParserFactory with RECORDS data source type must implement IRecordDataParserFactory
     *         <T>
     *         an instance of IDataParserFactory with STREAM data source type must implement IStreamDataParserFactory
     */
    public DataSourceType getDataSourceType();

    /**
     * Configure the data parser factory. The passed map contains key value pairs from the
     * submitted AQL statement and any additional pairs added by the compiler
     *
     * @param configuration
     */
    public void configure(Map<String, String> configuration) throws AlgebricksException;

    /**
     * Set the record type expected to be produced by parsers created by this factory
     *
     * @param recordType
     * @throws AsterixException
     *             if the parser does not support certain types defined in {@value recordType}.
     */
    public void setRecordType(ARecordType recordType) throws AsterixException;

    /**
     * Set the meta record type expected to be produced by parsers created by this factory
     *
     * @param metaType
     */
    public void setMetaType(ARecordType metaType);

    /**
     * Get the formats that are handled by this parser.
     *
     * @return A list of formats
     */
    public List<String> getParserFormats();
}
