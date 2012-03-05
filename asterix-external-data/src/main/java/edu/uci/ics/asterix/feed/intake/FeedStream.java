package edu.uci.ics.asterix.feed.intake;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;


public class FeedStream extends AbstractFeedStream {

	public FeedStream(IFeedClient feedClient, IHyracksTaskContext ctx)
			throws Exception {
		super(feedClient, ctx);
	}

}
