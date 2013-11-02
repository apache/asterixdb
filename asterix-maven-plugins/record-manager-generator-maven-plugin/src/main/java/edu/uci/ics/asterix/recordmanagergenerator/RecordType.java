/*
 * Copyright 2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.recordmanagergenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

public class RecordType {
    
    enum Type {
        BYTE,
        SHORT,
        INT,
        GLOBAL
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
        
        String methodName(String prefix) {
            String words[] = name.split(" ");
            assert(words.length > 0);
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
            sb.append("public ")
              .append(javaType(type))
              .append(' ')
              .append(methodName("get"))
              .append("(int slotNum) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final Buffer buf = buffers.get(slotNum / NO_SLOTS);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("buf.checkSlot(slotNum % NO_SLOTS);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buf.bb;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("return b.")
              .append(bbGetter(type))
              .append("((slotNum % NO_SLOTS) * ITEM_SIZE + ")
              .append(offsetName())
              .append(");\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
            
        StringBuilder appendMemoryManagerSetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public void ")
              .append(methodName("set"))
              .append("(int slotNum, ")
              .append(javaType(type))
              .append(" value) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("b.")
              .append(bbSetter(type))
              .append("((slotNum % NO_SLOTS) * ITEM_SIZE + ")
              .append(offsetName())
              .append(", value);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }

        StringBuilder appendArenaManagerGetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public ")
              .append(javaType(type))
              .append(' ')
              .append(methodName("get"))
              .append("(long slotNum) {\n");
            if (initial != null) {
              sb = indent(sb, indent, level + 1);
              sb.append("if (TRACK_ALLOC_ID) checkAllocId(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = RecordManagerTypes.Global.arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final int localId = RecordManagerTypes.Global.localId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("return get(arenaId).")
              .append(methodName("get"))
              .append("(localId);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
            
        StringBuilder appendArenaManagerSetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public void ")
              .append(methodName("set"))
              .append("(long slotNum, ")
              .append(javaType(type))
              .append(" value) {\n");
            if (initial != null) {
              sb = indent(sb, indent, level + 1);
              sb.append("if (TRACK_ALLOC_ID) checkAllocId(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = RecordManagerTypes.Global.arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final int localId = RecordManagerTypes.Global.localId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("get(arenaId).")
              .append(methodName("set"))
              .append("(localId, value);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
        
        StringBuilder appendInitializers(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("bb.")
              .append(bbSetter(type))
              .append("(slotNum * ITEM_SIZE + ")
              .append(offsetName())
              .append(", ");
            if (initial != null) {
                sb.append(initial);
            } else {
                sb.append(deadMemInitializer(type));
            }
            sb.append(");\n");
            return sb;
        }
        
        StringBuilder appendChecks(StringBuilder sb, String indent, int level) {
            if (initial == null) {
                return sb;
            }
            sb = indent(sb, indent, level);
            sb.append("if (bb.")
              .append(bbGetter(type))
              .append("(itemOffset + ")
              .append(offsetName())
              .append(") == ")
              .append(deadMemInitializer(type))
              .append(") {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("String msg = \"invalid value in field ")
              .append(offsetName())
              .append(" of slot \" + slotNum;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("throw new IllegalStateException(msg);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
        
        String offsetName() {
            String words[] = name.split(" ");
            assert(words.length > 0);
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
    
    public void addToMap(Map<String, RecordType> map) {
        modifiable = false;
        calcOffsetsAndSize();
        map.put(name, this);
    }

    public void addField(String name, Type type, String initial) {
        addField(name, type, initial, true);
    }    
    
    private void addField(String name, Type type, String initial, boolean accessible) {
        if (! modifiable) {
            throw new IllegalStateException("cannot modify type anmore");
        }
        fields.add(new Field(name, type, initial, -1, accessible));
    }
     
    private void calcOffsetsAndSize() {
        Collections.sort(fields, new Comparator<Field>() {
            public int compare(Field left, Field right) {
                return size(right.type) - size(left.type);
            }
        });
        // sort fields by size and align the items
        totalSize = 0;
        int alignment = 0;
        for (int i = 0; i < fields.size(); ++i) {            
            final Field field = fields.get(i);
            assert field.offset == -1;
            field.offset = totalSize;
            final int size = size(field.type);
            totalSize += size;
            if (size > alignment) alignment = size;
        }
        if (totalSize % alignment != 0) {
            totalSize = ((totalSize / alignment) + 1) * alignment; 
        }
    }
    
    int size() {
        return fields.size();
    }
    
    static int size(Type t) {
        switch(t) {
            case BYTE:   return 1;
            case SHORT:  return 2;
            case INT:    return 4;
            case GLOBAL: return 8;
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String javaType(Type t) {
        switch(t) {
            case BYTE:   return "byte";
            case SHORT:  return "short";
            case INT:    return "int";
            case GLOBAL: return "long";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String bbGetter(Type t) {
        switch(t) {
            case BYTE:   return "get";
            case SHORT:  return "getShort";
            case INT:    return "getInt";
            case GLOBAL: return "getLong";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String bbSetter(Type t) {
        switch(t) {
            case BYTE:   return "put";
            case SHORT:  return "putShort";
            case INT:    return "putInt";
            case GLOBAL: return "putLong";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String deadMemInitializer(Type t) {
        switch(t) {
            case BYTE:   return "(byte)0xde";
            case SHORT:  return "(short)0xdead";
            case INT:    return "0xdeadbeef";
            case GLOBAL: return "0xdeadbeefdeadbeefl";
            default:     throw new IllegalArgumentException();
        }        
    }
    
    static String appender(Type t) {
        switch(t) {
            case BYTE:   return "RecordManagerTypes.Byte.append";
            case SHORT:  return "RecordManagerTypes.Short.append";
            case INT:    return "RecordManagerTypes.Int.append";
            case GLOBAL: return "RecordManagerTypes.Global.append";
            default:     throw new IllegalArgumentException();
        }        
    }
    
    static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);  
    }

    static String padLeft(String s, int n) {
        return String.format("%1$" + n + "s", s);  
    }
    
    StringBuilder appendConstants(StringBuilder sb, String indent, int level) {
        sb = indent(sb, indent, level);
        sb.append("public static int ITEM_SIZE = ")
          .append(totalSize)
          .append(";\n");
        for (int i = 0; i < fields.size(); ++i) {
            final Field field = fields.get(i);
            sb = indent(sb, indent, level);
            sb.append("public static int ")
              .append(field.offsetName())
              .append(" = ")
              .append(field.offset).append("; // size: ")
              .append(size(field.type)).append("\n");
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
            sb.append("sb.append(\"")
              .append(padRight(field.name, maxNameWidth))
              .append(" | \");\n");
            sb = indent(sb, indent, level);
            sb.append("for (int i = 0; i < NO_SLOTS; ++i) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append(javaType(field.type))
              .append(" value = bb.")
              .append(bbGetter(field.type))
              .append("(i * ITEM_SIZE + ")
              .append(field.offsetName())
              .append(");\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb = ")
              .append(appender(field.type))
              .append("(sb, value);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb.append(\" | \");\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            sb = indent(sb, indent, level);
            sb.append("sb.append(\"\\n\");\n");
        }
        return sb;
    }
}
