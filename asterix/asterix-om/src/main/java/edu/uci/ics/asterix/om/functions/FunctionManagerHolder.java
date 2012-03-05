package edu.uci.ics.asterix.om.functions;

public class FunctionManagerHolder {
    private static IFunctionManager functionManager;

    public static IFunctionManager getFunctionManager() {
        return functionManager;
    }

    public static void setFunctionManager(IFunctionManager manager) {
        functionManager = manager;
    }
}