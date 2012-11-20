package edu.uci.ics.pregelix.runtime.bootstrap;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCApplicationEntryPoint;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

public class NCApplicationEntryPoint implements INCApplicationEntryPoint {
    @Override
    public void appMain(INCApplicationContext ncAppCtx, String[] args) throws Exception {
        RuntimeContext rCtx = new RuntimeContext(ncAppCtx);
        ncAppCtx.setApplicationObject(rCtx);
    }
}