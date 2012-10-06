package edu.uci.ics.hyracks.api.application;

/**
 * Implemented by the bootstrap class of the application that will manage its
 * life cycle at a Node Controller.
 * 
 * @author vinayakb
 * 
 */
public interface INCBootstrap extends IBootstrap {
    /**
     * Called by the infrastructure to set the NC Application Context for the
     * application. The infrastructure makes this call prior to calling start().
     * 
     * @param appCtx
     *            - The NC application context
     */
    public void setApplicationContext(INCApplicationContext appCtx);
}