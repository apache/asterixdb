package edu.uci.ics.asterix.feed.intake;

import java.util.List;

public interface IFeedClient {

	public boolean next(List<String> list);
}
