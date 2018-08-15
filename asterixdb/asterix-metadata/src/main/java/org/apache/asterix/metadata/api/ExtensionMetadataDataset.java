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
package org.apache.asterix.metadata.api;

import java.util.List;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.metadata.bootstrap.MetadataIndex;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;

public class ExtensionMetadataDataset<T> extends MetadataIndex {

    private static final long serialVersionUID = 1L;
    private final ExtensionMetadataDatasetId indexId;
    private final transient IMetadataEntityTupleTranslatorFactory<T> tupleTranslatorFactory;

    public ExtensionMetadataDataset(MetadataIndexImmutableProperties indexProperties, int numFields, IAType[] keyTypes,
            List<List<String>> keyNames, int numSecondaryIndexKeys, ARecordType payloadType, boolean isPrimaryIndex,
            int[] primaryKeyIndexes, ExtensionMetadataDatasetId indexId,
            IMetadataEntityTupleTranslatorFactory<T> tupleTranslatorFactory) {
        super(indexProperties, numFields, keyTypes, keyNames, numSecondaryIndexKeys, payloadType, isPrimaryIndex,
                primaryKeyIndexes);
        this.indexId = indexId;
        this.tupleTranslatorFactory = tupleTranslatorFactory;
    }

    public ExtensionMetadataDatasetId getId() {
        return indexId;
    }

    public IMetadataEntityTupleTranslator<T> getTupleTranslator() {
        return tupleTranslatorFactory.createTupleTranslator();
    }
}
