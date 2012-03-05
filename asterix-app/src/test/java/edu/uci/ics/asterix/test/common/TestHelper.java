package edu.uci.ics.asterix.test.common;

import java.util.List;

public final class TestHelper {

    public static boolean isInPrefixList(List<String> prefixList, String s) {
        for (String s2 : prefixList) {
            if (s.startsWith(s2)) {
                return true;
            }
        }
        return false;
    }

}
