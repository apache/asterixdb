package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class NodeLoad implements Comparable<NodeLoad> {

    private final String nodeId;

    private int nRuntimes;

    public NodeLoad(String nodeId) {
        this.nodeId = nodeId;
        this.nRuntimes = 0;
    }

    public void addLoad() {
        nRuntimes++;
    }

    public void removeLoad(FeedRuntimeType runtimeType) {
        nRuntimes--;
    }

    @Override
    public int compareTo(NodeLoad o) {
        if (this == o) {
            return 0;
        }
        return nRuntimes - o.getnRuntimes();
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getnRuntimes() {
        return nRuntimes;
    }

    public void setnRuntimes(int nRuntimes) {
        this.nRuntimes = nRuntimes;
    }

}
