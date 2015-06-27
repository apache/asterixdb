package edu.uci.ics.asterix.metadata.api;

import java.util.Set;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.cluster.Node;

public interface IClusterManager {

    /**
     * @param node
     * @throws AsterixException
     */
    public void addNode(Node node) throws AsterixException;

    /**
     * @param node
     * @throws AsterixException
     */
    public void removeNode(Node node) throws AsterixException;

    /**
     * @param subscriber
     */
    public void registerSubscriber(IClusterEventsSubscriber subscriber);

    /**
     * @param sunscriber
     * @return
     */
    public boolean deregisterSubscriber(IClusterEventsSubscriber sunscriber);

    /**
     * @return
     */
    public Set<IClusterEventsSubscriber> getRegisteredClusterEventSubscribers();

}
