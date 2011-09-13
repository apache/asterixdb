package edu.uci.ics.hyracks.adminconsole.client.rest;

import edu.uci.ics.hyracks.adminconsole.client.beans.NodeDetails;

public class GetNodeDetailsFunction extends AbstractRestFunction<NodeDetails> {
    private String nodeId;

    public GetNodeDetailsFunction(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    protected void appendURLPath(StringBuilder buffer) {
        buffer.append("/nodes/").append(nodeId);
    }
}