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
package org.apache.asterix.app.data.gen;

import java.io.IOException;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.TestTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class RecordTupleGenerator {

    private final int[] keyIndexes;
    private final int[] keyIndicators;
    private final ARecordValueGenerator recordGenerator;
    private final ARecordValueGenerator metaGenerator;
    private final TestTupleReference tuple;

    public enum GenerationFunction {
        RANDOM,
        DETERMINISTIC,
        INCREASING,
        DECREASING
    }

    /**
     * @param recordType
     * @param metaType
     * @param key
     * @param keyIndexes
     * @param keyIndicators
     * @param recordGeneration
     * @param uniqueRecordFields
     * @param metaGeneration
     * @param uniqueMetaFields
     */
    public RecordTupleGenerator(ARecordType recordType, ARecordType metaType, int[] keyIndexes, int[] keyIndicators,
            GenerationFunction[] recordGeneration, boolean[] uniqueRecordFields, GenerationFunction[] metaGeneration,
            boolean[] uniqueMetaFields) {
        this.keyIndexes = keyIndexes;
        this.keyIndicators = keyIndicators;
        for (IAType field : recordType.getFieldTypes()) {
            validate(field);
        }
        recordGenerator = new ARecordValueGenerator(recordGeneration, recordType, uniqueRecordFields, true);
        if (metaType != null) {
            for (IAType field : metaType.getFieldTypes()) {
                validate(field);
            }
            metaGenerator = new ARecordValueGenerator(metaGeneration, metaType, uniqueMetaFields, true);
        } else {
            metaGenerator = null;
        }
        int numOfFields = keyIndexes.length + 1 + ((metaType != null) ? 1 : 0);
        tuple = new TestTupleReference(numOfFields);
        boolean atLeastOneKeyFieldIsNotRandomAndNotBoolean = false;
        for (int i = 0; i < keyIndexes.length; i++) {
            if (keyIndicators[i] < 0 || keyIndicators[i] > 1) {
                throw new IllegalArgumentException("key field indicator must be either 0 or 1");
            }
            atLeastOneKeyFieldIsNotRandomAndNotBoolean = atLeastOneKeyFieldIsNotRandomAndNotBoolean
                    || validateKey(keyIndexes[i], keyIndicators[i] == 0 ? recordType : metaType,
                            keyIndicators[i] == 0 ? uniqueRecordFields[i] : uniqueMetaFields[i]);
        }
        if (!atLeastOneKeyFieldIsNotRandomAndNotBoolean) {
            throw new IllegalArgumentException("at least one key field must be unique and not boolean");
        }
        if (keyIndexes.length != keyIndicators.length) {
            throw new IllegalArgumentException("number of key indexes must equals number of key indicators");
        }
    }

    private boolean validateKey(int i, ARecordType type, boolean unique) {
        if (type.getFieldNames().length <= i) {
            throw new IllegalArgumentException("key index must be less than number of fields");
        }
        return unique && type.getFieldTypes()[i].getTypeTag() != ATypeTag.BOOLEAN;
    }

    public ITupleReference next() throws IOException {
        tuple.reset();
        recordGenerator.next(tuple.getFields()[keyIndexes.length].getDataOutput());
        if (metaGenerator != null) {
            recordGenerator.next(tuple.getFields()[keyIndexes.length + 1].getDataOutput());
        }
        for (int i = 0; i < keyIndexes.length; i++) {
            if (keyIndicators[i] == 0) {
                recordGenerator.get(keyIndexes[i], tuple.getFields()[i].getDataOutput());
            } else {
                metaGenerator.get(keyIndexes[i], tuple.getFields()[i].getDataOutput());
            }
        }
        return tuple;
    }

    private void validate(IAType field) {
        switch (field.getTypeTag()) {
            case BOOLEAN:
            case DOUBLE:
            case INTEGER:
            case BIGINT:
            case STRING:
                break;
            default:
                throw new IllegalArgumentException("Generating data of type " + field + " is not supported");
        }
    }
}
