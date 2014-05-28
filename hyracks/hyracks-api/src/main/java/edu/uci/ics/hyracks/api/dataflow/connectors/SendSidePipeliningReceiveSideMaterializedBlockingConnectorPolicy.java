package edu.uci.ics.hyracks.api.dataflow.connectors;

public class SendSidePipeliningReceiveSideMaterializedBlockingConnectorPolicy implements IConnectorPolicy {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresProducerConsumerCoscheduling() {
        return true;
    }

    @Override
    public boolean consumerWaitsForProducerToFinish() {
        return false;
    }

    @Override
    public boolean materializeOnSendSide() {
        return false;
    }

    @Override
    public boolean materializeOnReceiveSide() {
        return true;
    }

}
