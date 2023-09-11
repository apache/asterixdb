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

import java.util.List;
import java.util.Set;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IRecordReaderFactory<T> extends IExternalDataSourceFactory {

    IRecordReader<? extends T> createRecordReader(IExternalDataRuntimeContext context) throws HyracksDataException;

    Class<?> getRecordClass();

    @Override
    default DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    List<String> getRecordReaderNames();

    /**
     * Usually there is only a single adapter with a specific name.
     * When two or more adapters share the same name, only one can support {@link ExternalDataConstants#ALL_FORMATS}.
     * Other adapters must have only a subset of {@link ExternalDataConstants#ALL_FORMATS}.
     *
     * @return adapter's supported formats
     */
    default Set<String> getReaderSupportedFormats() {
        return ExternalDataConstants.ALL_FORMATS;
    }

}
