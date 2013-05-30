package edu.uci.ics.asterix.metadata.api;

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

}
