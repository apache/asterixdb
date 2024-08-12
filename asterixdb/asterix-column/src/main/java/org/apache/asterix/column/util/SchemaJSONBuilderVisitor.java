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
package org.apache.asterix.column.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import it.unimi.dsi.fastutil.ints.IntList;

public class SchemaJSONBuilderVisitor implements ISchemaNodeVisitor<JsonNode, Void> {
    private static final String TYPE_FIELD = "type";
    private static final String ITEM_FIELD = "item";
    private static final String ITEMS_FIELD = "items";
    private static final String FIELDS_FIELD = "fields";
    private static final String INDEX_FIELD = "index";
    private static final String LEVEL_FIELD = "level";
    private static final String OBJECT_TYPE = "object";
    private static final String ARRAY_TYPE = "array";
    private static final String UNION_TYPE = "union";
    private final List<String> fieldNames;
    private int level;

    public SchemaJSONBuilderVisitor(IFieldNamesDictionary dictionary) throws HyracksDataException {
        this.fieldNames = new ArrayList<>();
        AStringSerializerDeserializer stringSerDer =
                new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        List<IValueReference> extractedFieldNames = dictionary.getFieldNames();
        //Deserialize field names
        ByteArrayAccessibleInputStream in = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
        ByteArrayAccessibleDataInputStream dataIn = new ByteArrayAccessibleDataInputStream(in);
        for (IValueReference serFieldName : extractedFieldNames) {
            in.setContent(serFieldName.getByteArray(), 0, serFieldName.getLength());
            AString fieldName = stringSerDer.deserialize(dataIn);
            this.fieldNames.add(fieldName.getStringValue());
        }
        level = 0;
    }

    public String build(ObjectSchemaNode root) throws HyracksDataException {
        JsonNode jsonNode = visit(root, null);
        return jsonNode.toString();
    }

    @Override
    public JsonNode visit(ObjectSchemaNode objectNode, Void arg) throws HyracksDataException {
        List<AbstractSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.putIfAbsent(TYPE_FIELD, TextNode.valueOf(OBJECT_TYPE));
        jsonNode.putIfAbsent(LEVEL_FIELD, IntNode.valueOf(level));
        ObjectNode fieldsNode = JsonNodeFactory.instance.objectNode();
        level++;
        for (int i = 0; i < children.size(); i++) {
            int index = fieldNameIndexes.getInt(i);
            String fieldName = index < 0 ? "<empty>" : fieldNames.get(index);
            AbstractSchemaNode child = children.get(i);
            JsonNode childNode = child.accept(this, null);
            fieldsNode.putIfAbsent(fieldName, childNode);
        }
        level--;
        jsonNode.putIfAbsent(FIELDS_FIELD, fieldsNode);
        return jsonNode;
    }

    @Override
    public JsonNode visit(AbstractCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.putIfAbsent(TYPE_FIELD, TextNode.valueOf(ARRAY_TYPE));
        jsonNode.putIfAbsent(LEVEL_FIELD, IntNode.valueOf(level));
        AbstractSchemaNode itemNode = collectionNode.getItemNode();
        level++;
        jsonNode.putIfAbsent(ITEM_FIELD, itemNode.accept(this, null));
        level--;
        return jsonNode;
    }

    @Override
    public JsonNode visit(UnionSchemaNode unionNode, Void arg) throws HyracksDataException {
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.putIfAbsent(TYPE_FIELD, TextNode.valueOf(UNION_TYPE));
        jsonNode.putIfAbsent(LEVEL_FIELD, IntNode.valueOf(level));
        ArrayNode itemsNode = JsonNodeFactory.instance.arrayNode();
        for (AbstractSchemaNode child : unionNode.getChildren().values()) {
            itemsNode.add(child.accept(this, null));
        }
        jsonNode.putIfAbsent(ITEMS_FIELD, itemsNode);
        return jsonNode;
    }

    @Override
    public JsonNode visit(PrimitiveSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.putIfAbsent(TYPE_FIELD, TextNode.valueOf(primitiveNode.getTypeTag().toString()));
        jsonNode.putIfAbsent(LEVEL_FIELD, IntNode.valueOf(level));
        jsonNode.putIfAbsent(INDEX_FIELD, IntNode.valueOf(primitiveNode.getColumnIndex()));
        return jsonNode;
    }

}
