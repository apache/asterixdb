package edu.uci.ics.asterix.feed.intake;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public abstract class AbstractFeedStream extends InputStream {

	private ByteBuffer buffer;
	private int capacity;
	private IFeedClient feedClient;
	private List<String> feedObjects;

	public AbstractFeedStream(IFeedClient feedClient, IHyracksTaskContext ctx)
			throws Exception {
		capacity = ctx.getFrameSize();
		buffer = ByteBuffer.allocate(capacity);
		this.feedClient = feedClient;
		initialize();
	}

	@Override
	public int read() throws IOException {
		if (!buffer.hasRemaining()) {

			boolean hasMore = feedClient.next(feedObjects);
			if (!hasMore) {
				return -1;
			}
			buffer.position(0);
			buffer.limit(capacity);
			for (String feed : feedObjects) {
				buffer.put(feed.getBytes());
				buffer.put("\n".getBytes());
			}
			buffer.flip();
			return buffer.get();
		} else {
			return buffer.get();
		}

	}

	private void initialize() throws Exception {
		boolean hasMore = feedClient.next(feedObjects);
		if (!hasMore) {
			buffer.limit(0);
		} else {
			buffer.position(0);
			buffer.limit(capacity);
			for (String feed : feedObjects) {
				buffer.put(feed.getBytes());
				buffer.put("\n".getBytes());
			}
			buffer.flip();
		}
	}

}
