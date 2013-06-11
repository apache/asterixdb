package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;

public class AddNodeWork extends AbstractClusterManagementWork {

    private final int numberOfNodes;

    @Override
    public WorkType getClusterManagementWorkType() {
        return WorkType.ADD_NODE;
    }

    public AddNodeWork(int numberOfNodes, IClusterEventsSubscriber subscriber) {
        super(subscriber);
        this.numberOfNodes = numberOfNodes;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

}
