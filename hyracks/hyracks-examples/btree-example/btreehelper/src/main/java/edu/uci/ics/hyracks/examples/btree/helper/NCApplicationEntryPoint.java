package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCApplicationEntryPoint;

public class NCApplicationEntryPoint implements INCApplicationEntryPoint {
    @Override
    public void appMain(INCApplicationContext ncAppCtx, String[] args) throws Exception {
        RuntimeContext rCtx = new RuntimeContext(ncAppCtx);
        ncAppCtx.setApplicationObject(rCtx);
    }
}