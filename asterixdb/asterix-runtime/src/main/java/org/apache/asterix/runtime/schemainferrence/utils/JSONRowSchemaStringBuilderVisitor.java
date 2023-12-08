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
package org.apache.asterix.runtime.schemainferrence.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.UnionRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import it.unimi.dsi.fastutil.ints.IntList;

/*
Schema string builder for logging purpose
Implementation taken from column schema inference.
 */
public class JSONRowSchemaStringBuilderVisitor implements IRowSchemaNodeVisitor<Void, Void> {
    public static String RECORD_SCHEMA = "record";
    public static String META_RECORD_SCHEMA = "meta-record";
    private final StringBuilder builder;
    private final List<String> fieldNames;

    private int level;
    private int indent;

    public JSONRowSchemaStringBuilderVisitor(RowFieldNamesDictionary dictionary) throws HyracksDataException {
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

    public String build(ObjectRowSchemaNode root) throws HyracksDataException {
        builder.append("{ \n");
        builder.append("\"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n")
                .append("\"$id\": \"https://json-schema.org/draft/2020-12/schema\",\n");
        builder.append("\"type\":\"object\",\n");
        visit(root, null);
        //        appendPostDecor();
        builder.append(" }\n");
        return builder.toString();
    }

    @Override
    public Void visit(ObjectRowSchemaNode objectNode, Void arg) throws HyracksDataException {
        List<AbstractRowSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        level++;
        indent++;
        // Prop open
        builder.append("\"properties\": { \n");
        for (int i = 0; i < children.size(); i++) {

            int index = fieldNameIndexes.getInt(i);
            String fieldName = fieldNames.get(index);
            AbstractRowSchemaNode child = children.get(i);

            append(fieldName, index, child);
            if (child.getTypeTag() != ATypeTag.UNION) {
                if (child.isNested()) {
                    builder.append(",\n");
                } else {
                    builder.append("\n");
                }
            }
            child.accept(this, null);
            if (i != children.size() - 1 && child.getTypeTag() != ATypeTag.UNION) {
                builder.append(" },\n");
            } else if (child.getTypeTag() != ATypeTag.UNION) {
                builder.append(" }\n");
            } else if (child.isNested()) {
                builder.append(",\n");
            } else {
                builder.append("\n");
            }
        }
        // Prop close
        builder.append(" }\n");

        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(AbstractRowCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        level++;
        indent++;
        AbstractRowSchemaNode itemNode = collectionNode.getItemNode();
        builder.append("\"items\": ");
        builder.append("{ \n\"oneOf\": [\n");
        //        append("\"oneOf\": [  ", itemNode);
        if (itemNode.getTypeTag() != ATypeTag.UNION) {
            builder.append("{  \"type\":");
            writeSchemaType(getNormalizedTypeTag(itemNode.getTypeTag()));

            if (itemNode.isNested()) {
                builder.append(",\n");
            }
        }

        itemNode.accept(this, null);
        if (!itemNode.isNested()) {
            builder.append(" }\n");
        } else {
            builder.append("\n");
        }
        //        builder.append("}\n");
        builder.append("]\n").append(" }\n");
        //        appendPostDecor();
        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(UnionRowSchemaNode unionNode, Void arg) throws HyracksDataException {
        indent++;
        builder.append("{ \"oneOf\": [\n");
        List<AbstractRowSchemaNode> unionChildren =
                new ArrayList<AbstractRowSchemaNode>(unionNode.getChildren().values());
        //        for (AbstractRowSchemaNode child : unionNode.getChildren().values()) {
        for (int i = 0; i < unionChildren.size(); i++) {
            AbstractRowSchemaNode child = unionChildren.get(i);
            //            append(child.getTypeTag().toString(), child);
            if (child.getTypeTag() != ATypeTag.UNION) {
                builder.append("{  \"type\":");
                writeSchemaType(getNormalizedTypeTag(child.getTypeTag()));
                if (child.isNested()) {
                    builder.append(",\n");
                }
            }
            child.accept(this, null);
            if (i != unionChildren.size() - 1) {
                builder.append(" },\n");
            } else {
                builder.append(" }\n");
            }
        }
        builder.append("]\n }");
        //        builder.append("]\n");
        indent--;
        return null;
    }

    @Override
    public Void visit(PrimitiveRowSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        return null;
    }

    private void appendDecor() {
        builder.append("'properties':".repeat(Math.max(0, indent - 1)));
        builder.append("{ ");
    }

    private void appendPostDecor() {
        //        builder.append("|    ".repeat(Math.max(0, indent - 1)));
        builder.append("}\n");
    }

    private void append(String key, AbstractRowSchemaNode node) throws HyracksDataException {
        append(key, -1, node);
    }

    private void append(String key, int index, AbstractRowSchemaNode node) throws HyracksDataException {
        //        appendDecor();
        builder.append("\"").append(key).append("\"").append(": ");
        //        if (!node.isNested()) {
        //            final PrimitiveRowSchemaNode primitiveNode = (PrimitiveRowSchemaNode) node;
        if (node.getTypeTag() != ATypeTag.UNION) {
            builder.append("{ \n\"type\":");
            writeSchemaType(getNormalizedTypeTag(node.getTypeTag()));
        }
        //        }
        //        builder.append("\n");
    }

    public static ATypeTag getNormalizedTypeTag(ATypeTag typeTag) {
        switch (typeTag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ATypeTag.BIGINT;
            case FLOAT:
                return ATypeTag.DOUBLE;
            default:
                return typeTag;
        }
    }

    private void writeSchemaType(ATypeTag normalizedTypeTag) throws HyracksDataException {
        switch (normalizedTypeTag) {
            case OBJECT:
                builder.append("\"object\"");
                return;
            case MULTISET:
            case ARRAY:
                builder.append("\"array\"");
                return;
            case NULL:
            case MISSING:
                builder.append("\"null\"");
                return;
            case BOOLEAN:
                builder.append("\"boolean\"");
                return;
            case DOUBLE:
            case BIGINT:
                builder.append("\"integer\"");
                return;
            case UUID:
            case STRING:
                builder.append("\"string\"");
                return;
            case UNION:
                builder.append("\"union\"");
                return;
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }
    }
}
