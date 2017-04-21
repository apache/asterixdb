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
package org.apache.asterix.external.provider;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.ILookupReaderFactory;
import org.apache.asterix.external.input.record.reader.hdfs.HDFSLookupReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hyracks.api.application.IServiceContext;

public class LookupReaderFactoryProvider {

    @SuppressWarnings("rawtypes")
    public static ILookupReaderFactory getLookupReaderFactory(IServiceContext serviceCtx,
            Map<String, String> configuration) throws AsterixException {
        String inputFormat = HDFSUtils.getInputFormatClassName(configuration);
        if (inputFormat.equals(ExternalDataConstants.CLASS_NAME_TEXT_INPUT_FORMAT)
                || inputFormat.equals(ExternalDataConstants.CLASS_NAME_SEQUENCE_INPUT_FORMAT)
                || inputFormat.equals(ExternalDataConstants.CLASS_NAME_RC_INPUT_FORMAT)) {
            HDFSLookupReaderFactory<Object> readerFactory = new HDFSLookupReaderFactory<>();
            readerFactory.configure(serviceCtx, configuration);
            return readerFactory;
        } else {
            throw new AsterixException("Unrecognized external format");
        }
    }
}
