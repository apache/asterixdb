package edu.uci.ics.hyracks.control.nc.application;

import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public class NCApplicationContext extends ApplicationContext implements INCApplicationContext {
    private final String nodeId;
    private final IHyracksRootContext rootCtx;
    private Object appObject;

    public NCApplicationContext(ServerContext serverCtx, IHyracksRootContext rootCtx, String nodeId) throws IOException {
        super(serverCtx);
        this.nodeId = nodeId;
        this.rootCtx = rootCtx;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    public void setDistributedState(Serializable state) {
        distributedState = state;
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