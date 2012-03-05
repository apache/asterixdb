package edu.uci.ics.asterix.common.functions;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier; 
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionInfoImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

    private static Map<FunctionIdentifier, IFunctionInfo> _finfos = new HashMap<FunctionIdentifier, IFunctionInfo>();

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        IFunctionInfo finfo = _finfos.get(fi);
        if (finfo != null) {
            return finfo;
        }
        IFunctionInfo newF = new FunctionInfoImpl(fi);
        _finfos.put(fi, newF);
        return newF;
    }

}
