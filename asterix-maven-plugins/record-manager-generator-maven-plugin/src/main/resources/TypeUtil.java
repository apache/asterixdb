package @PACKAGE@;

public class TypeUtil {
    
    public static class Byte {
        public static StringBuilder append(StringBuilder sb, byte b) {
            return sb.append(String.format("%1$x", b));
        }

        public static StringBuilder appendFixed(StringBuilder sb, byte b) {
            return sb.append(String.format("%1$18x", b));
        }
    }

    public static class Short {
        public static StringBuilder append(StringBuilder sb, short s) {
            return sb.append(String.format("%1$x", s));
        }

        public static StringBuilder appendFixed(StringBuilder sb, short s) {
            return sb.append(String.format("%1$18x", s));
        }
    }

    public static class Int {
        public static StringBuilder append(StringBuilder sb, int i) {
            return sb.append(String.format("%1$x", i));
        }

        public static StringBuilder appendFixed(StringBuilder sb, int i) {
            return sb.append(String.format("%1$18x", i));
        }
    }

    public static class Global {

        public static long build(int arenaId, int allocId, int localId) {
            long result = arenaId;
            result = result << 48;
            result |= (((long)allocId) << 32);
            result |= localId;
            return result;
        }

        public static int arenaId(long l) {
            return (int)((l >>> 48) & 0xffff);
        }

        public static int allocId(long l) {
            return (int)((l >>> 32) & 0xffff);
        }

        public static int localId(long l) {
            return (int) (l & 0xffffffffL);
        }
        
        public static StringBuilder append(StringBuilder sb, long l) {
            sb.append(String.format("%1$x", TypeUtil.Global.arenaId(l)));
            sb.append(':');
            sb.append(String.format("%1$x", TypeUtil.Global.allocId(l)));
            sb.append(':');
            sb.append(String.format("%1$x", TypeUtil.Global.localId(l)));
            return sb;
        }
        
        public static StringBuilder appendFixed(StringBuilder sb, long l) {
            sb.append(String.format("%1$4x", TypeUtil.Global.arenaId(l)));
            sb.append(':');
            sb.append(String.format("%1$4x", TypeUtil.Global.allocId(l)));
            sb.append(':');
            sb.append(String.format("%1$8x", TypeUtil.Global.localId(l)));
            return sb;
        }
        
        public static String toString(long l) {
            return append(new StringBuilder(), l).toString();
        }
        
    }
}
