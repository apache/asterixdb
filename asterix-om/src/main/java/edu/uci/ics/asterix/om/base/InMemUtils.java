package edu.uci.ics.asterix.om.base;

public class InMemUtils {

    public final static boolean cursorEquals(IACursor c1, IACursor c2) {
        while (c1.next()) {
            if (!(c2.next())) {
                return false;
            }
            IAObject thisO = c1.get();
            IAObject otherO = c2.get();
            if (!(thisO.equals(otherO))) {
                return false;
            }
        }
        if (c2.next()) {
            return false;
        } else {
            return true;
        }
    }

    public final static int hashCursor(IACursor c) {
        int h = 0;
        while (c.next()) {
            h = h * 31 + c.get().hashCode();
        }
        return h;
    }

    static int hashDouble(double value) {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    static boolean deepEqualArrays(IAObject[] v1, IAObject[] v2) {
        if (v1.length != v2.length) {
            return false;
        }
        for (int i = 0; i < v1.length; i++) {
            if (!v1[i].deepEqual(v2[i])) {
                return false;
            }
        }
        return true;
    }
}
