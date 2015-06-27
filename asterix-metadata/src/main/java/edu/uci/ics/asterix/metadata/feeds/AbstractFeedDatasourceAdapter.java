package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;


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
