package edu.uci.ics.hyracks.control.nc.application;

import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCBootstrap;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public class NCApplicationContext extends ApplicationContext implements INCApplicationContext {
    private final IHyracksRootContext rootCtx;
    private Object appObject;

    public NCApplicationContext(ServerContext serverCtx, IHyracksRootContext rootCtx, String appName)
            throws IOException {
        super(serverCtx, appName);
        this.rootCtx = rootCtx;
    }

    @Override
    public void setDistributedState(Serializable state) {
        distributedState = state;
    }

    @Override
    protected void start() throws Exception {
        ((INCBootstrap) bootstrap).setApplicationContext(this);
        bootstrap.start();
    }

    @Override
    protected void stop() throws Exception {
        if (bootstrap != null) {
            bootstrap.stop();
        }
    }

    @Override
    public IHyracksRootContext getRootContext() {
        return rootCtx;
    }

    @Override
    public void setApplicationObject(Object object) {
        this.appObject = object;
    }

    @Override
    public Object getApplicationObject() {
        return appObject;
    }
}