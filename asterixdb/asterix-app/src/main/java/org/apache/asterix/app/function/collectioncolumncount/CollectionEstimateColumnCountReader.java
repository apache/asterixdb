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

package org.apache.asterix.app.function.collectioncolumncount;

import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class CollectionEstimateColumnCountReader extends FunctionReader {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String STORAGE_PARTITION = "id";
    private static final String COLUMN_COUNT = "columnCount";
    private final CharArrayRecord record;
    private final Int2IntMap estimatedColumnCount;
    private final IntIterator partitionIds;
    private final ObjectNode node;

    CollectionEstimateColumnCountReader(Int2IntMap estimatedColumnCount) {
        this.estimatedColumnCount = estimatedColumnCount;
        this.partitionIds = estimatedColumnCount.keySet().iterator();
        this.node = mapper.createObjectNode();
        this.record = new CharArrayRecord();
    }

    @Override
    public boolean hasNext() throws IOException {
        return partitionIds.hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        record.reset();
        int key = partitionIds.nextInt();
        int columnCount = estimatedColumnCount.get(key);
        node.removeAll();
        node.put(STORAGE_PARTITION, key);
        node.put(COLUMN_COUNT, columnCount);
        record.append(node.toString().toCharArray());
        record.endRecord();
        return record;
    }
}
