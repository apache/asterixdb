package edu.uci.ics.asterix.om.types;

import java.util.Hashtable;

/*
 * Author: Guangqiang Li
 * Created on Sep 24, 2009 
 */
public class TypeHierarchy {
    private static Hashtable<String, String> parentMap = new Hashtable<String, String>();
    static {
        parentMap.put("integer", "decimal");
        parentMap.put("double", "decimal");
        parentMap.put("decimal", "numeric");
    }

    public static boolean isSubType(String sub, String par) {
        String parent = parentMap.get(sub);
        if (parent != null)
            if (parent.equals(par))
                return true;
            else
                return isSubType(parent, par);
        return false;
    }
}
