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
import edu.uci.ics.asterix.event.util.PatternCreator;
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
            List<Pattern> pattern = new ArrayList<Pattern>();
            String asterixInstanceName = null;
            Patterns prepareNode = PatternCreator.INSTANCE.createPrepareNodePattern(asterixInstanceName,
                    AsterixClusterProperties.INSTANCE.getCluster(), node);
            String eventsHomeDir = cluster.getWorkingDir().getDir();
            cluster.getNode().add(node);
            EventrixClient client = new EventrixClient(eventsHomeDir, cluster, false, null);
            client.submit(prepareNode);

            pattern.clear();
            String ccHost = cluster.getMasterNode().getClusterIp();
            String hostId = node.getId();
            String nodeControllerId = asterixInstanceName + "_" + node.getId();
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            Pattern startNC = PatternCreator.INSTANCE.createNCStartPattern(ccHost, hostId, nodeControllerId, iodevices);
            pattern.add(startNC);
            Patterns startNCPattern = new Patterns(pattern);
            client.submit(startNCPattern);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    @Override
    public void removeNode(Node node) throws AsterixException {

    }

    private List<Pattern> getRemoveNodePattern(Node node) {
        List<Pattern> pattern = new ArrayList<Pattern>();

        return pattern;
    }
}
