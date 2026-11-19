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
package org.apache.asterix.column.metadata.schema.collection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * A collapsed representation of an object whose field-name set grows without bound (UUID keys, opaque IDs, dates used
 * as field names). Instead of one column per distinct key, the node holds:
 * <ul>
 * <li>one <b>key column</b> (STRING) carrying the field names as values, and</li>
 * <li>one <b>shared value node</b> (the inherited collection {@code item}) carrying every value.</li>
 * </ul>
 * For N distinct keys whose values each contain L leaves, this is {@code 1 + L} columns rather than {@code N * L} —
 * the count stops depending on N, which is the unbounded term.
 * <p>
 * Modelled as a collection because the physical encoding is identical: a map is a repeated (key, value) pair, so the
 * existing definition/repetition-level machinery applies unchanged, and {@link #accept} dispatches through the
 * existing {@code visit(AbstractCollectionSchemaNode)} so current visitors keep working.
 * <p>
 * Field names that land in the key column are <b>not</b> interned into the field-name dictionary, so the trie stops
 * growing too.
 */
public final class MapSchemaNode extends AbstractCollectionSchemaNode {
    private final PrimitiveSchemaNode keyNode;

    public MapSchemaNode(PrimitiveSchemaNode keyNode) {
        this.keyNode = keyNode;
        numberOfColumns = keyNode.getNumberOfColumns();
        previousNumberOfColumns = numberOfColumns;
    }

    public MapSchemaNode(DataInput input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        super();
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunLengthIntArray());
        }
        // Key first, then the shared value node, matching serialize() below.
        input.readByte(); // key node's type tag
        keyNode = new PrimitiveSchemaNode(ATypeTag.STRING, input);
        setItemNode(AbstractSchemaNode.deserialize(input, definitionLevels));
        numberOfColumns = keyNode.getNumberOfColumns() + getItemNode().getNumberOfColumns();
        previousNumberOfColumns = numberOfColumns;
    }

    /**
     * Query-side only: the field names a projection actually asked for. Never serialized and never present on a node
     * read back from storage — it exists solely on the clipped copy the clipper hands to the assembler, so that a
     * projected read can skip entries the query did not request instead of rebuilding the whole object.
     * <p>
     * Held untagged, matching the key column's stored form.
     */
    private List<IValueReference> requestedKeys;

    public PrimitiveSchemaNode getKeyNode() {
        return keyNode;
    }

    public List<IValueReference> getRequestedKeys() {
        return requestedKeys;
    }

    /** Builds a clipped view of this node that only surfaces the requested keys. */
    public MapSchemaNode clip(List<IValueReference> keys) {
        MapSchemaNode clipped = new MapSchemaNode(keyNode);
        clipped.setItemNode(getItemNode());
        clipped.requestedKeys = keys;
        return clipped;
    }

    public int getKeyColumnIndex() {
        return keyNode.getColumnIndex();
    }

    @Override
    public ATypeTag getTypeTag() {
        // SPARSOBJECT was declared but entirely unreferenced; it is claimed here for the collapsed-object form.
        return ATypeTag.SPARSOBJECT;
    }

    @Override
    public void serialize(DataOutput output, PathInfoSerializer pathInfoSerializer) throws IOException {
        output.write(getTypeTag().serialize());
        pathInfoSerializer.enter(this);
        // The key is recorded inside enter/exit because it lives in the map's repeated group, exactly like the value
        // node. Path info snapshots the current level and delimiters, and merge builds its column readers from it —
        // recording the key outside would give it the enclosing object's level and no delimiters, so merge would
        // read it as a non-repeated column.
        keyNode.serialize(output, pathInfoSerializer);
        getItemNode().serialize(output, pathInfoSerializer);
        pathInfoSerializer.exit(this);
    }
}
