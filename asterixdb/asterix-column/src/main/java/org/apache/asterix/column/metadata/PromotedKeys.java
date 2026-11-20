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
package org.apache.asterix.column.metadata;

import static org.apache.asterix.column.metadata.AbstractColumnMetadata.FIELD_NAMES_POINTER;
import static org.apache.asterix.column.metadata.AbstractColumnMetadata.META_SCHEMA_POINTER;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Lists the promoted keys of each collapsed node in the (newest) schema, and which ones a merge can move from the map
 * into their column.
 * <p>
 * A key is movable when its column is a single scalar type, not under an array or map, and the map has a value column
 * of that type. Each map entry is then one value a merge can copy across. Object values, unions and keys under arrays
 * stay in the map.
 */
public final class PromotedKeys {
    /** A collapsed node, identified by its map's key column. */
    public static final class Node {
        public final int mapKeyColumn;
        public final int numberOfChildren;
        public final List<Key> keys = new ArrayList<>();

        private Node(int mapKeyColumn, int numberOfChildren) {
            this.mapKeyColumn = mapKeyColumn;
            this.numberOfChildren = numberOfChildren;
        }
    }

    /** One promoted key of a {@link Node}. */
    public static final class Key {
        public final Node node;
        public final int childIndex;
        /** The key's column, or -1 when it has no single scalar column. */
        public final int column;
        /** The map's value column for the key's type; -1 if none (the key is not movable). */
        public final int valueColumn;
        /** The key's name as stored in the map's key column (untagged UTF-8). */
        public final IValueReference name;

        private Key(Node node, int childIndex, int column, int valueColumn, IValueReference name) {
            this.node = node;
            this.childIndex = childIndex;
            this.column = column;
            this.valueColumn = valueColumn;
            this.name = name;
        }

        public boolean isMovable() {
            return valueColumn >= 0;
        }
    }

    private PromotedKeys() {
    }

    /** The collapsed nodes in the schema, in a fixed order. */
    public static List<Node> of(IValueReference serializedMetadata) throws HyracksDataException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int fieldNamesStart = offset + IntegerPointable.getInteger(bytes, offset + FIELD_NAMES_POINTER);
        boolean hasMeta = IntegerPointable.getInteger(bytes, offset + META_SCHEMA_POINTER) >= 0;
        DataInput input =
                new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, serializedMetadata.getLength()));
        List<Node> nodes = new ArrayList<>();
        try {
            IFieldNamesDictionary dictionary = AbstractFieldNamesDictionary.deserialize(input);
            collect(AbstractSchemaNode.deserialize(input, null), dictionary, false, nodes);
            if (hasMeta) {
                collect(AbstractSchemaNode.deserialize(input, null), dictionary, false, nodes);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return nodes;
    }

    /** The movable keys of {@code nodes}. Their list index is shared by the merge's readers and writer. */
    public static List<Key> movable(List<Node> nodes) {
        List<Key> movable = new ArrayList<>();
        for (Node node : nodes) {
            for (Key key : node.keys) {
                if (key.isMovable()) {
                    movable.add(key);
                }
            }
        }
        return movable;
    }

    private static void collect(AbstractSchemaNode node, IFieldNamesDictionary dictionary, boolean repeated,
            List<Node> nodes) {
        if (node instanceof ObjectSchemaNode object) {
            MapSchemaNode map = object.getMapNode();
            if (map != null) {
                nodes.add(collapsedNode(object, map, dictionary, repeated));
            }
            for (AbstractSchemaNode child : object.getChildren()) {
                collect(child, dictionary, repeated, nodes);
            }
        } else if (node instanceof AbstractCollectionSchemaNode collection) {
            if (collection.getItemNode() != null) {
                collect(collection.getItemNode(), dictionary, true, nodes);
            }
        } else if (node instanceof UnionSchemaNode union) {
            for (AbstractSchemaNode child : union.getChildren().values()) {
                collect(child, dictionary, repeated, nodes);
            }
        }
    }

    private static Node collapsedNode(ObjectSchemaNode object, MapSchemaNode map, IFieldNamesDictionary dictionary,
            boolean repeated) {
        List<AbstractSchemaNode> children = object.getChildren();
        Node node = new Node(map.getKeyColumnIndex(), children.size());
        IntList fieldNameIndexes = object.getChildrenFieldNameIndexes();
        // Promoted children are the ones after the map (see ObjectSchemaNode#isPromoted).
        for (int i = children.indexOf(map) + 1; i < children.size(); i++) {
            AbstractSchemaNode child = children.get(i);
            int column = -1;
            int valueColumn = -1;
            if (child instanceof PrimitiveSchemaNode primitive && primitive.getTypeTag() != ATypeTag.MISSING) {
                column = primitive.getColumnIndex();
                valueColumn = repeated ? -1 : valueColumn(map.getItemNode(), primitive.getTypeTag());
            }
            node.keys.add(new Key(node, i, column, valueColumn,
                    dictionary.getFieldName(fieldNameIndexes.getInt(i))));
        }
        return node;
    }

    /** The column of {@code values} for {@code typeTag} (directly or in a union); -1 if none. */
    private static int valueColumn(AbstractSchemaNode values, ATypeTag typeTag) {
        if (values instanceof UnionSchemaNode union) {
            values = union.getChild(typeTag);
        }
        return values instanceof PrimitiveSchemaNode primitive && primitive.getTypeTag() == typeTag
                ? primitive.getColumnIndex() : -1;
    }
}
