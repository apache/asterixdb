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
package org.apache.asterix.column;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleReaderWriterFactory;
import org.apache.asterix.column.operation.lsm.load.LoadColumnTupleReaderWriterFactory;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnTupleReaderWriterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReaderWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class ColumnManagerFactory implements IColumnManagerFactory {
    private static final long serialVersionUID = -5003159552950739235L;
    private final ARecordType datasetType;
    private final ARecordType metaType;
    private final List<List<String>> primaryKeys;
    private final List<Integer> keySourceIndicator;
    private final int pageSize;
    private final int maxTupleCount;
    private final double tolerance;
    private final int maxLeafNodeSize;

    public ColumnManagerFactory(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
            List<Integer> keySourceIndicator, int pageSize, int maxTupleCount, double tolerance, int maxLeafNodeSize) {
        this.pageSize = pageSize;
        this.maxTupleCount = maxTupleCount;
        this.tolerance = tolerance;
        this.maxLeafNodeSize = maxLeafNodeSize;

        this.datasetType = datasetType;
        if (containsSplitKeys(keySourceIndicator)) {
            throw new UnsupportedOperationException(
                    "Primary keys split between meta-type and datasetType is not supported");
        }
        this.keySourceIndicator = keySourceIndicator;
        this.metaType = metaType;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public IColumnManager createColumnManager() {
        return new ColumnManager(datasetType, metaType, primaryKeys, keySourceIndicator);
    }

    @Override
    public AbstractColumnTupleReaderWriterFactory getLoadColumnTupleReaderWriterFactory(
            IBinaryComparatorFactory[] cmpFactories) {
        return new LoadColumnTupleReaderWriterFactory(pageSize, maxTupleCount, tolerance, maxLeafNodeSize,
                cmpFactories);
    }

    @Override
    public AbstractColumnTupleReaderWriterFactory getFlushColumnTupleReaderWriterFactory() {
        return new FlushColumnTupleReaderWriterFactory(pageSize, maxTupleCount, tolerance, maxLeafNodeSize);
    }

    @Override
    public AbstractColumnTupleReaderWriterFactory createMergeColumnTupleReaderWriterFactory() {
        return new MergeColumnTupleReaderWriterFactory(pageSize, maxTupleCount, tolerance, maxLeafNodeSize);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.putPOJO("datasetType", datasetType.toJson(registry));
        if (metaType != null) {
            json.putPOJO("metaType", metaType.toJson(registry));
        }

        json.put("pageSize", pageSize);
        json.put("maxTupleCount", maxTupleCount);
        json.put("tolerance", tolerance);
        json.put("maxLeafNodeSize", maxLeafNodeSize);

        ArrayNode primaryKeysArray = json.putArray("primaryKeys");
        for (List<String> primaryKey : primaryKeys) {
            ArrayNode primaryKeyArray = primaryKeysArray.addArray();
            for (String path : primaryKey) {
                primaryKeyArray.add(path);
            }
        }

        ArrayNode keySourceIndicatorNode = json.putArray("keySourceIndicator");
        for (int keySourceIndicatorInt : keySourceIndicator) {
            keySourceIndicatorNode.add(keySourceIndicatorInt);
        }
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        ARecordType datasetType = (ARecordType) registry.deserialize(json.get("datasetType"));
        JsonNode metaItemTypeNode = json.get("metaType");
        ARecordType metaType = null;
        if (metaItemTypeNode != null) {
            metaType = (ARecordType) registry.deserialize(metaItemTypeNode);
        }

        int pageSize = json.get("pageSize").asInt();
        int maxTupleCount = json.get("maxTupleCount").asInt();
        double tolerance = json.get("tolerance").asDouble();
        int maxLeafNodeSize = json.get("maxLeafNodeSize").asInt();

        List<List<String>> primaryKeys = new ArrayList<>();
        ArrayNode primaryKeysNode = (ArrayNode) json.get("primaryKeys");
        for (int i = 0; i < primaryKeysNode.size(); i++) {
            List<String> primaryKey = new ArrayList<>();
            ArrayNode primaryKeyNode = (ArrayNode) primaryKeysNode.get(i);
            for (int j = 0; j < primaryKeyNode.size(); j++) {
                primaryKey.add(primaryKeyNode.get(j).asText());
            }
            primaryKeys.add(primaryKey);
        }

        List<Integer> keySourceIndicator = new ArrayList<>();
        ArrayNode keySourceIndicatorNode = (ArrayNode) json.get("keySourceIndicator");
        for (int i = 0; i < keySourceIndicatorNode.size(); i++) {
            keySourceIndicator.add(keySourceIndicatorNode.get(i).asInt());
        }

        return new ColumnManagerFactory(datasetType, metaType, primaryKeys, keySourceIndicator, pageSize, maxTupleCount,
                tolerance, maxLeafNodeSize);
    }

    private static boolean containsSplitKeys(List<Integer> keySourceIndicator) {
        if (keySourceIndicator.size() == 1) {
            return false;
        }
        Integer value = keySourceIndicator.get(0);
        for (int i = 1; i < keySourceIndicator.size(); i++) {
            if (!Objects.equals(value, keySourceIndicator.get(i))) {
                return true;
            }
        }
        return false;
    }

}
