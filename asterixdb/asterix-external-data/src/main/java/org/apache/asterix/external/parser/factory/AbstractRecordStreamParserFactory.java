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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.api.IStreamDataParserFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.types.ARecordType;

public abstract class AbstractRecordStreamParserFactory<T>
        implements IStreamDataParserFactory, IRecordDataParserFactory<T> {

    private static final long serialVersionUID = 1L;
    protected ARecordType recordType;
    protected Map<String, String> configuration;

    @Override
    public DataSourceType getDataSourceType() {
        return ExternalDataUtils.getDataSourceType(configuration);
    }

    @Override
    public void configure(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void setRecordType(ARecordType recordType) throws AsterixException {
        this.recordType = recordType;
    }
}
