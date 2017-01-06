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

package org.apache.asterix.recordmanagergenerator;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RecordType {

    enum Type {
        BYTE(1, "byte", "get", "put", "(byte)0xde", "TypeUtil.Byte.append", "TypeUtil.Byte.appendFixed"),
        SHORT(
                2,
                "short",
                "getShort",
                "putShort",
                "(short)0xdead",
                "TypeUtil.Short.append",
                "TypeUtil.Short.appendFixed"),
        INT(4, "int", "getInt", "putInt", "0xdeadbeef", "TypeUtil.Int.append", "TypeUtil.Int.appendFixed"),
        GLOBAL(
                8,
                "long",
                "getLong",
                "putLong",
                "0xdeadbeefdeadbeefl",
                "TypeUtil.Global.append",
                "TypeUtil.Global.appendFixed");

        Type(int size, String javaType, String bbGetter, String bbSetter, String deadMemInitializer, String appender,
                String tabAppender) {
            this.size = size;
            this.javaType = javaType;
            this.bbGetter = bbGetter;
            this.bbSetter = bbSetter;
            this.deadMemInitializer = deadMemInitializer;
            this.appender = appender;
            this.tabAppender = tabAppender;
        }

        int size;
        String javaType;
        String bbGetter;
        String bbSetter;
        String deadMemInitializer;
        String appender;
        String tabAppender;
    }

    static class Field {

        String name;
        Type type;
        String initial;
        int offset;
        boolean accessible = true;

        Field(String name, Type type, String initial, int offset, boolean accessible) {
            this.name = name;
            this.type = type;
            this.initial = initial;
            this.offset = offset;
            this.accessible = accessible;
        }

        public static Field fromJSON(JsonNode obj) {
            String name = obj.get("name").asText();
            Type type = parseType(obj.get("type").asText());
            String initial = obj.get("initial") == null ? null : obj.get("initial").asText();
            return new Field(name, type, initial, -1, true);
        }

        private static Type parseType(String string) {
            string = string.toUpperCase();
            if (string.equals("GLOBAL")) {
                return Type.GLOBAL;
            } else if (string.equals("INT")) {
                return Type.INT;
            } else if (string.equals("SHORT")) {
                return Type.SHORT;
            } else if (string.equals("BYTE")) {
                return Type.BYTE;
            }
            throw new IllegalArgumentException("Unknown type \"" + string + "\"");
        }

        String methodName(String prefix) {
            String words[] = name.split(" ");
            assert words.length > 0;
            StringBuilder sb = new StringBuilder(prefix);
            for (int j = 0; j < words.length; ++j) {
                String word = words[j];
                sb.append(word.substring(0, 1).toUpperCase());
                sb.append(word.substring(1));
            }
            return sb.toString();
        }

        StringBuilder appendMemoryManagerGetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public ").append(type.javaType).append(' ').append(methodName("get"))
                    .append("(int slotNum) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final Buffer buf = buffers.get(slotNum / NO_SLOTS);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("buf.checkSlot(slotNum % NO_SLOTS);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buf.bb;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("return b.").append(type.bbGetter).append("((slotNum % NO_SLOTS) * ITEM_SIZE + ")
                    .append(offsetName()).append(");\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        StringBuilder appendMemoryManagerSetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public void ").append(methodName("set")).append("(int slotNum, ").append(type.javaType)
                    .append(" value) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("b.").append(type.bbSetter).append("((slotNum % NO_SLOTS) * ITEM_SIZE + ").append(offsetName())
                    .append(", value);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        StringBuilder appendArenaManagerGetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public ").append(type.javaType).append(' ').append(methodName("get"))
                    .append("(long slotNum) {\n");
            if (initial != null) {
                sb = indent(sb, indent, level + 1);
                sb.append("if (TRACK_ALLOC_ID) checkAllocId(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = TypeUtil.Global.arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final int localId = TypeUtil.Global.localId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("return get(arenaId).").append(methodName("get")).append("(localId);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        StringBuilder appendArenaManagerSetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public void ").append(methodName("set")).append("(long slotNum, ").append(type.javaType)
                    .append(" value) {\n");
            if (initial != null) {
                sb = indent(sb, indent, level + 1);
                sb.append("if (TRACK_ALLOC_ID) checkAllocId(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = TypeUtil.Global.arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final int localId = TypeUtil.Global.localId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("get(arenaId).").append(methodName("set")).append("(localId, value);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        StringBuilder appendInitializers(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("bb.").append(type.bbSetter).append("(slotNum * ITEM_SIZE + ").append(offsetName()).append(", ");
            if (initial != null) {
                sb.append(initial);
            } else {
                sb.append(type.deadMemInitializer);
            }
            sb.append(");\n");
            return sb;
        }

        StringBuilder appendChecks(StringBuilder sb, String indent, int level) {
            if (initial == null) {
                return sb;
            }
            sb = indent(sb, indent, level);
            sb.append("if (bb.").append(type.bbGetter).append("(itemOffset + ").append(offsetName()).append(") == ")
                    .append(type.deadMemInitializer).append(") {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("String msg = \"invalid value in field ").append(offsetName())
                    .append(" of slot \" + TypeUtil.Global.toString(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("throw new IllegalStateException(msg);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        String offsetName() {
            String words[] = name.split(" ");
            assert (words.length > 0);
            StringBuilder sb = new StringBuilder(words[0].toUpperCase());
            for (int j = 1; j < words.length; ++j) {
                sb.append("_").append(words[j].toUpperCase());
            }
            sb.append("_OFF");
            return sb.toString();
        }

        int offset() {
            return offset;
        }
    }

    String name;
    ArrayList<Field> fields;
    int totalSize;
    boolean modifiable = true;

    static StringBuilder indent(StringBuilder sb, String indent, int level) {
        for (int i = 0; i < level; ++i) {
            sb.append(indent);
        }
        return sb;
    }

    public RecordType(String name) {
        this.name = name;
        fields = new ArrayList<Field>();
        addField("next free slot", Type.INT, "-1", false);
    }

    public static RecordType read(Reader reader) throws IOException {
        ObjectNode node = new ObjectMapper().readValue(reader, ObjectNode.class);
        return fromJSON(node);
    }

    public static RecordType fromJSON(ObjectNode obj) {
        RecordType result = new RecordType(obj.get("name").asText());
        JsonNode fields = obj.get("fields");
        for (int i = 0; i < fields.size(); i++) {
            JsonNode n = fields.get(i);
            result.fields.add(Field.fromJSON(n));
        }
        return result;
    }

    public void addToMap(Map<String, RecordType> map) {
        modifiable = false;
        calcOffsetsAndSize();
        map.put(name, this);
    }

    public void addField(String name, Type type, String initial) {
        addField(name, type, initial, true);
    }

    private void addField(String name, Type type, String initial, boolean accessible) {
        if (!modifiable) {
            throw new IllegalStateException("cannot modify type anmore");
        }
        fields.add(new Field(name, type, initial, -1, accessible));
    }

    private void calcOffsetsAndSize() {
        Collections.sort(fields, new Comparator<Field>() {
            public int compare(Field left, Field right) {
                return right.type.size - left.type.size;
            }
        });
        // sort fields by size and align the items
        totalSize = 0;
        int alignment = 0;
        for (int i = 0; i < fields.size(); ++i) {
            final Field field = fields.get(i);
            assert field.offset == -1;
            field.offset = totalSize;
            final int size = field.type.size;
            totalSize += size;
            if (size > alignment) {
                alignment = size;
            }
        }
        if (totalSize % alignment != 0) {
            totalSize = ((totalSize / alignment) + 1) * alignment;
        }
    }

    int size() {
        return fields.size();
    }

    static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    static String padLeft(String s, int n) {
        return String.format("%1$" + n + "s", s);
    }

    StringBuilder appendConstants(StringBuilder sb, String indent, int level) {
        sb = indent(sb, indent, level);
        sb.append("public static int ITEM_SIZE = ").append(totalSize).append(";\n");
        for (int i = 0; i < fields.size(); ++i) {
            final Field field = fields.get(i);
            sb = indent(sb, indent, level);
            sb.append("public static int ").append(field.offsetName()).append(" = ").append(field.offset)
                    .append("; // size: ").append(field.type.size).append("\n");
        }
        return sb;
    }

    StringBuilder appendBufferPrinter(StringBuilder sb, String indent, int level) {
        int maxNameWidth = 0;
        for (int i = 0; i < fields.size(); ++i) {
            int width = fields.get(i).name.length();
            if (width > maxNameWidth) {
                maxNameWidth = width;
            }
        }
        for (int i = 0; i < fields.size(); ++i) {
            final Field field = fields.get(i);
            sb = indent(sb, indent, level);
            sb.append("sb.append(\"").append(padRight(field.name, maxNameWidth)).append(" | \");\n");
            sb = indent(sb, indent, level);
            sb.append("for (int i = 0; i < NO_SLOTS; ++i) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append(field.type.javaType).append(" value = bb.").append(field.type.bbGetter)
                    .append("(i * ITEM_SIZE + ").append(field.offsetName()).append(");\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb = ").append(field.type.tabAppender).append("(sb, value);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb.append(\" | \");\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            sb = indent(sb, indent, level);
            sb.append("sb.append(\"\\n\");\n");
        }
        return sb;
    }

    StringBuilder appendRecordPrinter(StringBuilder sb, String indent, int level) {
        sb = indent(sb, indent, level);
        sb.append("public StringBuilder appendRecord(StringBuilder sb, long slotNum) {\n");

        sb = indent(sb, indent, level + 1);
        sb.append("sb.append(\"{ \");\n\n");

        for (int i = 0; i < fields.size(); ++i) {
            Field field = fields.get(i);
            if (field.accessible) {
                if (i > 0) {
                    sb = indent(sb, indent, level + 1);
                    sb.append("sb.append(\", \");\n\n");
                }
                sb = indent(sb, indent, level + 1);
                sb.append("sb.append(\"\\\"").append(field.name).append("\\\" : \\\"\");\n");
                sb = indent(sb, indent, level + 1);
                sb.append("sb = ").append(field.type.appender).append("(sb, ");
                sb.append(field.methodName("get")).append("(slotNum)");
                sb.append(");\n");
                sb = indent(sb, indent, level + 1);
                sb.append("sb.append(\"\\\"\");\n\n");
            }
        }
        sb = indent(sb, indent, level + 1);
        sb.append("return sb.append(\" }\");\n");

        sb = indent(sb, indent, level);
        sb.append("}");

        return sb;
    }
}
