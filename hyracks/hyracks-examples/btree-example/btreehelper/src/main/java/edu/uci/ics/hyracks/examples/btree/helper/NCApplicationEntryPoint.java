package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCApplicationEntryPoint;

public class NCApplicationEntryPoint implements INCApplicationEntryPoint {
    @Override
    public void start(INCApplicationContext ncAppCtx, String[] args) throws Exception {
        RuntimeContext rCtx = new RuntimeContext(ncAppCtx);
        ncAppCtx.setApplicationObject(rCtx);
    }

    @Override
    public void notifyStartupComplete() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}