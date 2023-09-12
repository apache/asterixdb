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

package org.apache.asterix.metadata.bootstrap;

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_FULL_TEXT_CONFIG;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_CONFIG_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_FILTER_PIPELINE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_TOKENIZER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_FULL_TEXT_CONFIG;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class FullTextConfigEntity {

    private static final FullTextConfigEntity FULL_TEXT_CONFIG = new FullTextConfigEntity(
            new MetadataIndex(PROPERTIES_FULL_TEXT_CONFIG, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_FULL_TEXT_CONFIG_NAME)), 0,
                    fullTextConfigType(), true, new int[] { 0, 1 }),
            2, -1);

    private static final FullTextConfigEntity DB_FULL_TEXT_CONFIG =
            new FullTextConfigEntity(new MetadataIndex(PROPERTIES_FULL_TEXT_CONFIG, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_FULL_TEXT_CONFIG_NAME)),
                    0, databaseFullTextConfigType(), true, new int[] { 0, 1, 2 }), 3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int configNameIndex;
    private final int tokenizerIndex;
    private final int filterPipelineIndex;

    private FullTextConfigEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.configNameIndex = startIndex++;
        this.tokenizerIndex = startIndex++;
        this.filterPipelineIndex = startIndex++;;
    }

    public static FullTextConfigEntity of(boolean cloudDeployment) {
        return FULL_TEXT_CONFIG;
    }

    public MetadataIndex getIndex() {
        return index;
    }

    public ARecordType getRecordType() {
        return index.getPayloadRecordType();
    }

    public int payloadPosition() {
        return payloadPosition;
    }

    public int databaseNameIndex() {
        return databaseNameIndex;
    }

    public int dataverseNameIndex() {
        return dataverseNameIndex;
    }

    public int configNameIndex() {
        return configNameIndex;
    }

    public int tokenizerIndex() {
        return tokenizerIndex;
    }

    public int filterPipelineIndex() {
        return filterPipelineIndex;
    }

    private static ARecordType fullTextConfigType() {
        return MetadataRecordTypes
                .createRecordType(RECORD_NAME_FULL_TEXT_CONFIG,
                        new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FULL_TEXT_CONFIG_NAME,
                                FIELD_NAME_FULL_TEXT_TOKENIZER, FIELD_NAME_FULL_TEXT_FILTER_PIPELINE },
                        new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                                AUnionType.createNullableType(BuiltinType.ASTRING), AUnionType.createNullableType(
                                        new AOrderedListType(BuiltinType.ASTRING, "FullTextFilterPipeline")) },
                        true);
    }

    private static ARecordType databaseFullTextConfigType() {
        return MetadataRecordTypes
                .createRecordType(RECORD_NAME_FULL_TEXT_CONFIG,
                        new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME,
                                FIELD_NAME_FULL_TEXT_CONFIG_NAME, FIELD_NAME_FULL_TEXT_TOKENIZER,
                                FIELD_NAME_FULL_TEXT_FILTER_PIPELINE },
                        new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                                AUnionType.createNullableType(BuiltinType.ASTRING), AUnionType.createNullableType(
                                        new AOrderedListType(BuiltinType.ASTRING, "FullTextFilterPipeline")) },
                        true);
    }
}
