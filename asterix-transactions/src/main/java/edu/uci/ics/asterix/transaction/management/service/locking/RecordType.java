package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;

public class RecordType {
    
    enum Type {
        BYTE,
        SHORT,
        INT
    }
    
    static class Field {
        
        String name;
        Type type;
        String initial;
        int offset;

        Field(String name, Type type, String initial, int offset) {
            this.name = name;
            this.type = type;
            this.initial = initial;
            this.offset = offset;
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
              .append(name(type))
              .append(' ')
              .append(methodName("get"))
              .append("(int slotNum) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;\n");
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
              .append(name(type))
              .append(" value) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("  b.")
              .append(bbSetter(type))
              .append("((slotNum % NO_SLOTS) * ITEM_SIZE + ")
              .append(offsetName())
              .append(", value);\n");
            sb = indent(sb, indent, level);
            sb.append(indent)
              .append("}\n");
            return sb;
        }

        StringBuilder appendArenaManagerSetThreadLocal(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("final int arenaId = arenaId(slotNum);\n");
            sb = indent(sb, indent, level);
            sb.append("if (arenaId != local.get().arenaId) {\n");
            sb = indent(sb, indent, level + 1);
            sb.append("local.get().arenaId = arenaId;\n");
            sb = indent(sb, indent, level + 1);
            sb.append("local.get().mgr = get(arenaId);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
        
        StringBuilder appendArenaManagerGetMethod(StringBuilder sb, String indent, int level) {
            sb = indent(sb, indent, level);
            sb.append("public ")
              .append(name(type))
              .append(' ')
              .append(methodName("get"))
              .append("(int slotNum) {\n");
            sb = appendArenaManagerSetThreadLocal(sb, indent, level + 1);
            sb = indent(sb, indent, level + 1);
            sb.append("return local.get().mgr.")
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
              .append("(int slotNum, ")
              .append(name(type))
              .append(" value) {\n");
            sb = appendArenaManagerSetThreadLocal(sb, indent, level + 1);
            sb = indent(sb, indent, level + 1);
            sb.append("local.get().mgr.")
              .append(methodName("set"))
              .append("(localId(slotNum), value);\n");
            sb = indent(sb, indent, level);
            sb.append("}\n");
            return sb;
        }
        
        StringBuilder appendInitializers(StringBuilder sb, String indent, int level) {
            if (initial != null) {
                sb = indent(sb, indent, level);
                sb.append("bb.")
                  .append(bbSetter(type))
                  .append("(currentSlot * ITEM_SIZE + ")
                  .append(offsetName())
                  .append(", ")
                  .append(initial)
                  .append(");\n");
            }
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
        fields.add(new Field(name, type, initial, totalSize));
        totalSize += size(type);
    }
     
    int size() {
        return fields.size();
    }
    
    static int size(Type t) {
        switch(t) {
            case BYTE:  return 1;
            case SHORT: return 2;
            case INT:   return 4;
            default:    throw new IllegalArgumentException();
        }
    }
    
    static String name(Type t) {
        switch(t) {
            case BYTE:  return "byte";
            case SHORT: return "short";
            case INT:   return "int";
            default:    throw new IllegalArgumentException();
        }
    }
    
    static String bbGetter(Type t) {
        switch(t) {
            case BYTE:  return "get";
            case SHORT: return "getShort";
            case INT:   return "getInt";
            default:    throw new IllegalArgumentException();
        }
    }
    
    static String bbSetter(Type t) {
        switch(t) {
            case BYTE:  return "put";
            case SHORT: return "putShort";
            case INT:   return "putInt";
            default:    throw new IllegalArgumentException();
        }
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
}
