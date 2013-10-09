package edu.uci.ics.asterix.metadata.feeds;

public abstract class AbstractFeedDatasourceAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    protected FeedPolicyEnforcer policyEnforcer;

    public FeedPolicyEnforcer getPolicyEnforcer() {
        return policyEnforcer;
    }

    public void setFeedPolicyEnforcer(FeedPolicyEnforcer policyEnforcer) {
        this.policyEnforcer = policyEnforcer;
    }

}
