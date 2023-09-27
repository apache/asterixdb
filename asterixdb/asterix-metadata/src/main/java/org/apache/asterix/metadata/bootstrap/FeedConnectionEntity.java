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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_FEED_CONNECTION;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_APPLIED_FUNCTIONS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FEED_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_POLICY_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_RETURN_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_FEED_CONNECTION;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class FeedConnectionEntity {

    private static final FeedConnectionEntity FEED_CONNECTION = new FeedConnectionEntity(
            new MetadataIndex(PROPERTIES_FEED_CONNECTION, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_FEED_NAME),
                            List.of(FIELD_NAME_DATASET_NAME)),
                    0, feedConnectionType(), true, new int[] { 0, 1, 2 }),
            3, -1);

    private static final FeedConnectionEntity DB_FEED_CONNECTION =
            new FeedConnectionEntity(new MetadataIndex(PROPERTIES_FEED_CONNECTION, 5,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_FEED_NAME), List.of(FIELD_NAME_DATASET_NAME)),
                    0, databaseFeedConnectionType(), true, new int[] { 0, 1, 2, 3 }), 4, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int feedNameIndex;
    private final int datasetNameIndex;
    private final int outputTypeIndex;
    private final int appliedFunctionsIndex;
    private final int policyIndex;

    private FeedConnectionEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.feedNameIndex = startIndex++;
        this.datasetNameIndex = startIndex++;
        this.outputTypeIndex = startIndex++;
        this.appliedFunctionsIndex = startIndex++;
        this.policyIndex = startIndex++;
    }

    public static FeedConnectionEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_FEED_CONNECTION : FEED_CONNECTION;
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

    public int feedNameIndex() {
        return feedNameIndex;
    }

    public int datasetNameIndex() {
        return datasetNameIndex;
    }

    public int outputTypeIndex() {
        return outputTypeIndex;
    }

    public int appliedFunctionsIndex() {
        return appliedFunctionsIndex;
    }

    public int policyIndex() {
        return policyIndex;
    }

    private static ARecordType feedConnectionType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_FEED_CONNECTION,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FEED_NAME, FIELD_NAME_DATASET_NAME,
                        FIELD_NAME_RETURN_TYPE, FIELD_NAME_APPLIED_FUNCTIONS, FIELD_NAME_POLICY_NAME },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AUnorderedListType(BuiltinType.ASTRING, null), BuiltinType.ASTRING },
                //IsOpen?
                true);
    }

    private static ARecordType databaseFeedConnectionType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_FEED_CONNECTION,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FEED_NAME,
                        FIELD_NAME_DATASET_NAME, FIELD_NAME_RETURN_TYPE, FIELD_NAME_APPLIED_FUNCTIONS,
                        FIELD_NAME_POLICY_NAME },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, new AUnorderedListType(BuiltinType.ASTRING, null), BuiltinType.ASTRING },
                //IsOpen?
                true);
    }
}
