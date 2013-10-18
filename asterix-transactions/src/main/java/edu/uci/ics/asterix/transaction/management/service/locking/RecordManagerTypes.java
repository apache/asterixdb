package edu.uci.ics.asterix.transaction.management.service.locking;

public class RecordManagerTypes {
    
    public static class Byte {
        public static StringBuilder append(StringBuilder sb, byte b) {
            return sb.append(String.format("%1$18x", b));
        }
    }

    public static class Short {
        public static StringBuilder append(StringBuilder sb, short s) {
            return sb.append(String.format("%1$18x", s));
        }
    }

    public static class Int {
        public static StringBuilder append(StringBuilder sb, int i) {
            return sb.append(String.format("%1$18x", i));
        }
    }

    public static class Global {

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
            sb.append(String.format("%1$4x", RecordManagerTypes.Global.arenaId(l)));
            sb.append(':');
            sb.append(String.format("%1$4x", RecordManagerTypes.Global.allocId(l)));
            sb.append(':');
            sb.append(String.format("%1$8x", RecordManagerTypes.Global.localId(l)));
            return sb;
        }
        
        public static String toString(long l) {
            return append(new StringBuilder(), l).toString();
        }
        
    }
}
