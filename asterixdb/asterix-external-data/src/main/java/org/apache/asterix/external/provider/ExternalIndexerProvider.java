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
import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.indexing.FileOffsetIndexer;
import org.apache.asterix.external.indexing.RecordColumnarIndexer;
import org.apache.asterix.external.util.ExternalDataConstants;

public class ExternalIndexerProvider {

    public static IExternalIndexer getIndexer(Map<String, String> configuration) throws AsterixException {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        if (inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.INPUT_FORMAT_TEXT)
                || inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.CLASS_NAME_TEXT_INPUT_FORMAT)
                || inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.INPUT_FORMAT_SEQUENCE)
                || inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.CLASS_NAME_SEQUENCE_INPUT_FORMAT)) {
            return new FileOffsetIndexer();
        } else if (inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.INPUT_FORMAT_RC)
                || inputFormatParameter.equalsIgnoreCase(ExternalDataConstants.CLASS_NAME_RC_INPUT_FORMAT)) {
            return new RecordColumnarIndexer();
        } else {
            throw new AsterixException("Unable to create indexer for data with format: " + inputFormatParameter);
        }
    }
}
