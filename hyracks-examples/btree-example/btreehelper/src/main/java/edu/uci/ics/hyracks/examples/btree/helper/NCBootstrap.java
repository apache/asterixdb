package edu.uci.ics.hyracks.examples.btree.helper;

import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCBootstrap;

public class NCBootstrap implements INCBootstrap {
    private static final Logger LOGGER = Logger.getLogger(NCBootstrap.class.getName());

    private INCApplicationContext appCtx;

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting NC Bootstrap");
        RuntimeContext.initialize();
        LOGGER.info("Initialized RuntimeContext: " + RuntimeContext.getInstance());
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping Asterix NC Bootstrap");
        RuntimeContext.deinitialize();
    }

    @Override
    public void setApplicationContext(INCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }
}