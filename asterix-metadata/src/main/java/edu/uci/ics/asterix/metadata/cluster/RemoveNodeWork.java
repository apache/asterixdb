package edu.uci.ics.asterix.metadata.cluster;

import java.util.Set;

import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;

public class RemoveNodeWork extends AbstractClusterManagementWork {

    private final Set<String> nodesToBeRemoved;

    @Override
    public WorkType getClusterManagementWorkType() {
        return WorkType.REMOVE_NODE;
    }

    public RemoveNodeWork(Set<String> nodesToBeRemoved, IClusterEventsSubscriber subscriber) {
        super(subscriber);
        this.nodesToBeRemoved = nodesToBeRemoved;
    }

    public Set<String> getNodesToBeRemoved() {
        return nodesToBeRemoved;
    }

}
