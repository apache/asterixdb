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

import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import it.unimi.dsi.fastutil.ints.IntList;

public class SchemaStringBuilderVisitor implements ISchemaNodeVisitor<Void, Void> {
    public static String RECORD_SCHEMA = "record";
    public static String META_RECORD_SCHEMA = "meta-record";
    private final StringBuilder builder;
    private final List<String> fieldNames;

    private int level;
    private int indent;

    public SchemaStringBuilderVisitor(FieldNamesDictionary dictionary) throws HyracksDataException {
        builder = new StringBuilder();
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
        indent = 0;
    }

    public String build(ObjectSchemaNode root) throws HyracksDataException {
        builder.append("root\n");
        visit(root, null);
        return builder.toString();
    }

    @Override
    public Void visit(ObjectSchemaNode objectNode, Void arg) throws HyracksDataException {
        List<AbstractSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        level++;
        indent++;

        for (int i = 0; i < children.size(); i++) {
            int index = fieldNameIndexes.getInt(i);
            String fieldName = index < 0 ? "<empty>" : fieldNames.get(index);
            AbstractSchemaNode child = children.get(i);
            append(fieldName, index, child);
            child.accept(this, null);
        }

        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(AbstractCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        level++;
        indent++;
        AbstractSchemaNode itemNode = collectionNode.getItemNode();
        append("item", itemNode);
        itemNode.accept(this, null);
        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(UnionSchemaNode unionNode, Void arg) throws HyracksDataException {
        indent++;
        for (AbstractSchemaNode child : unionNode.getChildren().values()) {
            append(child.getTypeTag().toString(), child);
            child.accept(this, null);
        }
        indent--;
        return null;
    }

    @Override
    public Void visit(PrimitiveSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        return null;
    }

    private void appendDecor() {
        builder.append("|    ".repeat(Math.max(0, indent - 1)));
        builder.append("|-- ");
    }

    private void append(String key, AbstractSchemaNode node) {
        append(key, -1, node);
    }

    private void append(String key, int index, AbstractSchemaNode node) {
        appendDecor();
        builder.append(key);
        if (index >= 0) {
            builder.append(" (");
            builder.append(index);
            builder.append(')');
        }
        builder.append(": ");
        builder.append(node.getTypeTag().toString());
        builder.append(" <level: ");
        builder.append(level);
        if (!node.isNested()) {
            final PrimitiveSchemaNode primitiveNode = (PrimitiveSchemaNode) node;
            builder.append(", index: ");
            builder.append(primitiveNode.getColumnIndex());
        }
        builder.append(">\n");
    }
}
