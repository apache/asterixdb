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

public class RecordType {
    
    enum Type {
        BYTE,
        SHORT,
        INT,
        LOCAL,
        GLOBAL
    }
    
    static class Field {
        
        String name;
        Type type;
        String initial;
        int offset;
        boolean debugField;

        Field(String name, Type type, String initial, int offset, boolean debugField) {
            this.name = name;
            this.type = type;
            this.initial = initial;
            this.offset = offset;
            this.debugField = debugField;
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
            if (! debugField) {
              sb = indent(sb, indent, level + 1);
              sb.append("if (TRACK_ALLOC) checkSlot(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("return get(arenaId).")
              .append(methodName("get"))
              .append("(localId(slotNum));\n");
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
            if (! debugField) {
              sb = indent(sb, indent, level + 1);
              sb.append("if (TRACK_ALLOC) checkSlot(slotNum);\n");
            }
            sb = indent(sb, indent, level + 1);
            sb.append("final int arenaId = arenaId(slotNum);\n");
            sb = indent(sb, indent, level + 1);
            sb.append("get(arenaId).")
              .append(methodName("set"))
              .append("(localId(slotNum), value);\n");
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
            if (debugField) {
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
    
    static StringBuilder indent(StringBuilder sb, String indent, int level) {
        for (int i = 0; i < level; ++i) {
            sb.append(indent);
        }
        return sb;
    }
    
    RecordType(String name) {
        this.name = name;
        fields = new ArrayList<Field>();
        totalSize = 0;
    }
    
    void addField(String name, Type type, String initial) {
        addField(name, type, initial, false);
    }
     
    void addField(String name, Type type, String initial, boolean debugField) {
        fields.add(new Field(name, type, initial, totalSize, debugField));
        totalSize += size(type);
    }
     
    int size() {
        return fields.size();
    }
    
    static int size(Type t) {
        switch(t) {
            case BYTE:   return 1;
            case SHORT:  return 2;
            case INT:    return 4;
            case LOCAL:  return 4;
            case GLOBAL: return 8;
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String javaType(Type t) {
        switch(t) {
            case BYTE:   return "byte";
            case SHORT:  return "short";
            case INT:    return "int";
            case LOCAL:  return "int";
            case GLOBAL: return "long";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String bbGetter(Type t) {
        switch(t) {
            case BYTE:   return "get";
            case SHORT:  return "getShort";
            case INT:    return "getInt";
            case LOCAL:  return "getInt";
            case GLOBAL: return "getLong";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String bbSetter(Type t) {
        switch(t) {
            case BYTE:   return "put";
            case SHORT:  return "putShort";
            case INT:    return "putInt";
            case LOCAL:  return "putInt";
            case GLOBAL: return "putLong";
            default:     throw new IllegalArgumentException();
        }
    }
    
    static String deadMemInitializer(Type t) {
        switch(t) {
            case BYTE:   return "(byte)0xde";
            case SHORT:  return "(short)0xdead";
            case INT:    return "0xdeadbeef";
            case LOCAL:  return "0xdeadbeef";
            case GLOBAL: return "0xdeadbeefdeadbeefl";
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
              .append(field.offset).append(";\n");
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
            sb.append("sb.append(String.format(\"%1$2x\", ")
              .append(name)
              .append("ArenaManager.arenaId(value)));\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb.append(\" \");\n");
            sb = indent(sb, indent, level + 1);
            sb.append("sb.append(String.format(\"%1$6x\", ")
              .append(name)
              .append("ArenaManager.localId(value)));\n");
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
