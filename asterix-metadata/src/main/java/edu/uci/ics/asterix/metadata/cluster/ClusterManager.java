package edu.uci.ics.asterix.metadata.cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.config.AsterixClusterProperties;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.metadata.api.IClusterManager;

public class ClusterManager implements IClusterManager {

    public static ClusterManager INSTANCE = new ClusterManager();

    private static String eventsDir = System.getenv("user.dir") + File.separator + "eventrix";

    private ClusterManager() {
       
    }

    @Override
    public void addNode(Node node) throws AsterixException {
        try {
            Cluster cluster = AsterixClusterProperties.INSTANCE.getCluster();
            EventrixClient client = new EventrixClient(eventsDir, cluster, false, null);
            Patterns patterns = new Patterns();
            patterns.setPattern(getAddNodePattern(cluster, node));
            client.submit(patterns);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    @Override
    public void removeNode(Node node) throws AsterixException {

    }

    private List<Pattern> getAddNodePattern(Cluster cluster, Node node) {
        List<Pattern> pattern = new ArrayList<Pattern>();
        return pattern;
    }

    private List<Pattern> getRemoveNodePattern(Node node) {
        List<Pattern> pattern = new ArrayList<Pattern>();
        return pattern;
    }
}
