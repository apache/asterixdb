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
package org.apache.asterix.external.input.record.converter;

import java.util.Arrays;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordConverter;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;

public class CSVWithRecordConverterFactory implements IRecordConverterFactory<char[], RecordWithMetadataAndPK<char[]>> {

    private static final long serialVersionUID = 1L;
    private int recordIndex;
    private char delimiter;
    private ARecordType metaType;
    private ARecordType recordType;
    private int[] keyIndicators;
    private int[] keyIndexes;
    private IAType[] keyTypes;

    @Override
    public IRecordConverter<char[], RecordWithMetadataAndPK<char[]>> createConverter() {
        return new CSVToRecordWithMetadataAndPKConverter(recordIndex, delimiter, metaType, recordType, keyIndicators,
                keyIndexes, keyTypes);
    }

    @Override
    public void configure(final Map<String, String> configuration) throws AsterixException {
        //validate and set
        String property = configuration.get(ExternalDataConstants.KEY_RECORD_INDEX);
        if (property == null) {
            throw new AsterixException(
                    "Unspecified " + ExternalDataConstants.KEY_RECORD_INDEX + " for csv to csv with record converter");
        }
        recordIndex = Integer.parseInt(property);
        property = configuration.get(ExternalDataConstants.KEY_DELIMITER);
        if (property == null) {
            throw new AsterixException(
                    "Unspecified " + ExternalDataConstants.KEY_DELIMITER + " for csv to csv with record converter");
        }
        if (property.trim().length() > 1) {
            throw new AsterixException("Large delimiter. The maximum delimiter size = 1");
        }
        delimiter = property.trim().charAt(0);
        // only works for top level keys
        property = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        if (property == null) {
            keyIndexes = null;
            keyIndicators = null;
            keyTypes = null;
        } else {
            final String[] indexes = property.split(",");
            keyIndexes = new int[indexes.length];
            for (int i = 0; i < keyIndexes.length; i++) {
                keyIndexes[i] = Integer.parseInt(indexes[i].trim());
            }
            // default key indicators point to meta part
            property = configuration.get(ExternalDataConstants.KEY_KEY_INDICATORS);
            if (property == null) {
                keyIndicators = new int[keyIndexes.length];
                Arrays.fill(keyIndicators, 1);
            } else {
                keyIndicators = new int[keyIndexes.length];
                final String[] indicators = property.split(",");
                for (int i = 0; i < keyIndicators.length; i++) {
                    keyIndicators[i] = Integer.parseInt(indicators[i].trim());
                    if ((keyIndicators[i] > 1) || (keyIndicators[i] < 0)) {
                        throw new AsterixException("Invalid " + ExternalDataConstants.KEY_KEY_INDICATORS
                                + " value. Allowed values are only 0 and 1.");
                    }
                }
                keyTypes = new IAType[keyIndexes.length];
                for (int i = 0; i < keyIndicators.length; i++) {
                    if (keyIndicators[i] == 0) {
                        keyTypes[i] = recordType.getFieldTypes()[keyIndexes[i]];
                    } else {
                        keyTypes[i] = metaType.getFieldTypes()[keyIndexes[i]];
                    }
                }
            }
        }
    }

    @Override
    public Class<?> getInputClass() {
        return char[].class;
    }

    @Override
    public Class<?> getOutputClass() {
        return char[].class;
    }

    @Override
    public void setRecordType(final ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public void setMetaType(final ARecordType metaType) {
        this.metaType = metaType;
    }
}
