package edu.uci.ics.hyracks.api.application;

/**
 * Implemented by the bootstrap class of the application that will manage its
 * life cycle at the Cluster Controller.
 * 
 * @author vinayakb
 * 
 */
public interface ICCBootstrap extends IBootstrap {
    /**
     * Called by the infrastructure to set the CC Application Context for the
     * application. The infrastructure makes this call prior to calling start().
     * 
     * @param appCtx
     *            - The CC application context
     */
    public void setApplicationContext(ICCApplicationContext appCtx);
}