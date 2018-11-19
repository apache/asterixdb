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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.input.record.converter.CSVWithRecordConverterFactory;
import org.apache.asterix.external.input.record.converter.DCPConverterFactory;
import org.apache.asterix.external.input.record.converter.IRecordConverterFactory;
import org.apache.asterix.external.util.ExternalDataConstants;

@SuppressWarnings("rawtypes")
public class RecordConverterFactoryProvider {

    public static IRecordConverterFactory getConverterFactory(String format, String recordFormat)
            throws AsterixException {
        switch (recordFormat) {
            case ExternalDataConstants.FORMAT_ADM:
            case ExternalDataConstants.FORMAT_JSON_LOWER_CASE:
                // converter that produces records of adm/json type
                switch (format) {
                    case ExternalDataConstants.FORMAT_CSV:
                    case ExternalDataConstants.FORMAT_DELIMITED_TEXT:
                        return new CSVWithRecordConverterFactory();
                    case ExternalDataConstants.FORMAT_DCP:
                        return new DCPConverterFactory();
                }
        }
        throw new AsterixException("Unknown Converter Factory that can convert from " + format + " to " + recordFormat);
    }
}
