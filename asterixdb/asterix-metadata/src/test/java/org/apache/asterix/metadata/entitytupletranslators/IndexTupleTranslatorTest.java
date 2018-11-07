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
package org.apache.asterix.metadata.entitytupletranslators;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.junit.Assert;
import org.junit.Test;

public class IndexTupleTranslatorTest {

    @Test
    public void test() throws AlgebricksException, IOException {
        Integer[] indicators = { 0, 1, null };
        for (Integer indicator : indicators) {
            Map<String, String> compactionPolicyProperties = new HashMap<>();
            compactionPolicyProperties.put("max-mergable-component-size", "1073741824");
            compactionPolicyProperties.put("max-tolerance-component-count", "3");

            InternalDatasetDetails details = new InternalDatasetDetails(FileStructure.BTREE, PartitioningStrategy.HASH,
                    Collections.singletonList(Collections.singletonList("row_id")),
                    Collections.singletonList(Collections.singletonList("row_id")),
                    indicator == null ? null : Collections.singletonList(indicator),
                    Collections.singletonList(BuiltinType.AINT64), false, Collections.emptyList());

            Dataset dataset = new Dataset("test", "d1", "foo", "LogType", "CB", "MetaType", "DEFAULT_NG_ALL_NODES",
                    "prefix", compactionPolicyProperties, details, Collections.emptyMap(), DatasetType.INTERNAL, 115, 0,
                    CompressionManager.NONE);

            Index index = new Index("test", "d1", "i1", IndexType.BTREE,
                    Collections.singletonList(Collections.singletonList("row_id")),
                    indicator == null ? null : Collections.singletonList(indicator),
                    Collections.singletonList(BuiltinType.AINT64), -1, false, false, false, 0);

            MetadataNode mockMetadataNode = mock(MetadataNode.class);
            when(mockMetadataNode.getDatatype(any(), anyString(), anyString())).thenReturn(new Datatype("test", "d1",
                    new ARecordType("", new String[] { "row_id" }, new IAType[] { BuiltinType.AINT64 }, true), true));
            when(mockMetadataNode.getDataset(any(), anyString(), anyString())).thenReturn(dataset);

            IndexTupleTranslator idxTranslator = new IndexTupleTranslator(null, mockMetadataNode, true);
            ITupleReference tuple = idxTranslator.getTupleFromMetadataEntity(index);
            Index deserializedIndex = idxTranslator.getMetadataEntityFromTuple(tuple);
            if (indicator == null) {
                Assert.assertEquals(Collections.singletonList(new Integer(0)),
                        deserializedIndex.getKeyFieldSourceIndicators());
            } else {
                Assert.assertEquals(index.getKeyFieldSourceIndicators(),
                        deserializedIndex.getKeyFieldSourceIndicators());
            }
        }
    }
}
