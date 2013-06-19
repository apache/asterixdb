/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class AsterixClusterProperties {

    private static final Logger LOGGER = Logger.getLogger(AsterixClusterProperties.class.getName());

    private static final String IO_DEVICES = "iodevices";

    public static final AsterixClusterProperties INSTANCE = new AsterixClusterProperties();

    private Map<String, Map<String, String>> ncConfiguration = new HashMap<String, Map<String, String>>();

    public static final String CLUSTER_CONFIGURATION_FILE = "cluster.xml";
    private final Cluster cluster;

    private AsterixClusterProperties() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(CLUSTER_CONFIGURATION_FILE);
        if (is != null) {
            try {
                JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                cluster = (Cluster) unmarshaller.unmarshal(is);

            } catch (JAXBException e) {
                throw new IllegalStateException("Failed to read configuration file " + CLUSTER_CONFIGURATION_FILE);
            }
        } else {
            cluster = null;
        }
    }

    public enum State {
        ACTIVE,
        UNUSABLE
    }

    private State state = State.UNUSABLE;

    public void removeNCConfiguration(String nodeId) {
        // state = State.UNUSABLE;
        ncConfiguration.remove(nodeId);
    }

    public void addNCConfiguration(String nodeId, Map<String, String> configuration) {
        ncConfiguration.put(nodeId, configuration);
        if (ncConfiguration.keySet().size() == AsterixAppContextInfo.getInstance().getMetadataProperties()
                .getNodeNames().size()) {
            state = State.ACTIVE;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Registering configuration parameters for node id" + nodeId);
        }
    }

    /**
     * Returns the number of IO devices configured for a Node Controller
     * 
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return number of IO devices. -1 if the node id is not valid. A node id
     *         is not valid if it does not correspond to the set of registered
     *         Node Controllers.
     */
    public int getNumberOfIODevices(String nodeId) {
        Map<String, String> ncConfig = ncConfiguration.get(nodeId);
        if (ncConfig == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Configuration parameters for nodeId" + nodeId
                        + " not found. The node has not joined yet or has left.");
            }
            return -1;
        }
        return ncConfig.get(IO_DEVICES).split(",").length;
    }

    public State getState() {
        return state;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Node getAvailableSubstitutionNode() {
        List<Node> subNodes = cluster.getSubstituteNodes().getNode();
        return subNodes.isEmpty() ? null : subNodes.get(0);
    }

}
