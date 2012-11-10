package edu.uci.ics.asterix.feed.operator;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.feed.comm.AlterFeedMessage;
import edu.uci.ics.asterix.feed.comm.IFeedMessage;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IMutableFeedAdapter;
import edu.uci.ics.asterix.feed.mgmt.FeedId;
import edu.uci.ics.asterix.feed.mgmt.FeedSystemProvider;
import edu.uci.ics.asterix.feed.mgmt.IFeedManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class FeedIntakeOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IDatasourceAdapter adapter;
    private final int partition;
    private final IFeedManager feedManager;
    private final FeedId feedId;
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private FeedInboxMonitor feedInboxMonitor;

    public FeedIntakeOperatorNodePushable(FeedId feedId, IDatasourceAdapter adapter, int partition) {
        this.adapter = adapter;
        this.partition = partition;
        this.feedManager = (IFeedManager) FeedSystemProvider.getFeedManager();
        this.feedId = feedId;
        inbox = new LinkedBlockingQueue<IFeedMessage>();
    }

    @Override
    public void open() throws HyracksDataException {
        if (adapter instanceof IManagedFeedAdapter) {
            feedInboxMonitor = new FeedInboxMonitor((IManagedFeedAdapter) adapter, inbox, partition);
            feedInboxMonitor.start();
            feedManager.registerFeedOperatorMsgQueue(feedId, inbox);
        }
        writer.open();
        try {
            adapter.start(partition, writer);
        } catch (Exception e) {
            e.printStackTrace();
            // we do not throw an exception, but allow the operator to close
            // gracefully
            // Throwing an exception here would result in a job abort and a
            // transaction roll back
            // that undoes all the work done so far.

        } finally {
            writer.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // TODO Auto-generated method stub
    }
}

class FeedInboxMonitor extends Thread {

    private LinkedBlockingQueue<IFeedMessage> inbox;
    private final IManagedFeedAdapter adapter;

    public FeedInboxMonitor(IManagedFeedAdapter adapter, LinkedBlockingQueue<IFeedMessage> inbox, int partition) {
        this.inbox = inbox;
        this.adapter = adapter;
    }

    @Override
    public void run() {
        while (true) {
            try {
                IFeedMessage feedMessage = inbox.take();
                switch (feedMessage.getMessageType()) {
                    case SUSPEND:
                        adapter.suspend();
                        break;
                    case RESUME:
                        adapter.resume();
                        break;
                    case STOP:
                        adapter.stop();
                        break;
                    case ALTER:
                        ((IMutableFeedAdapter) adapter).alter(((AlterFeedMessage) feedMessage).getAlteredConfParams());
                        break;
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}