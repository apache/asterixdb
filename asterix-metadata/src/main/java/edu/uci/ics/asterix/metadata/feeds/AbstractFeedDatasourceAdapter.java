package edu.uci.ics.asterix.metadata.feeds;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;

public abstract class AbstractFeedDatasourceAdapter extends AbstractDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    private int batchPersistSize = 10;
    protected FeedPolicyEnforcer policyEnforcer;
    protected StatisticsCollector collector;
    protected Timer timer;

    public FeedPolicyEnforcer getPolicyEnforcer() {
        return policyEnforcer;
    }

    public void setFeedPolicyEnforcer(FeedPolicyEnforcer policyEnforcer) {
        this.policyEnforcer = policyEnforcer;
        FeedPolicyAccessor policyAccessor = this.policyEnforcer.getFeedPolicyAccessor();
        if (policyAccessor.collectStatistics()) {
            long period = policyAccessor.getStatisicsCollectionPeriodInSecs();
            collector = new StatisticsCollector(this, policyEnforcer.getFeedId(), period, batchPersistSize);
            timer = new Timer();
            timer.schedule(collector, period * 1000, period * 1000);
        }
    }

    public abstract long getIngestedRecordsCount();

    protected void dumpStatistics() throws RemoteException, ACIDException {
        collector.persistStatistics(true);
    }

    private static class StatisticsCollector extends TimerTask {

        private final AbstractFeedDatasourceAdapter adapter;
        private final FeedId feedId;
        //   private List<Long> previousCountValues = new ArrayList<Long>();
        //   private List<Integer> ingestionRates = new ArrayList<Integer>();
        private final long period;
        private FeedActivity feedActivity;
        private int numMeasurements = 0;
        private int batchPersistSize;
        private StringBuilder count = new StringBuilder();
        private StringBuilder rate = new StringBuilder();
        private long previousCount = 0;

        public StatisticsCollector(AbstractFeedDatasourceAdapter adapter, FeedId feedId, long period,
                int batchPersistSize) {
            this.adapter = adapter;
            this.feedId = feedId;
            this.period = period;
            this.batchPersistSize = batchPersistSize;
            this.feedActivity = new FeedActivity(feedId.getDataverse(), feedId.getDataset(),
                    FeedActivityType.FEED_STATS, new HashMap<String, String>());
        }

        @Override
        public void run() {
            try {
                persistStatistics(false);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public void persistStatistics(boolean force) throws RemoteException, ACIDException {
            MetadataTransactionContext ctx = null;
            try {
                long currentCount = adapter.getIngestedRecordsCount();
                long diff = (currentCount - previousCount);
                count.append(currentCount + ",");
                rate.append(new Double(diff / period).intValue() + ",");
                feedActivity.getFeedActivityDetails().put(FeedActivityDetails.INGESTION_RATE, "" + rate.toString());
                feedActivity.getFeedActivityDetails().put(FeedActivityDetails.TOTAL_INGESTED, "" + count.toString());
                numMeasurements++;
                if (numMeasurements == batchPersistSize || force) {
                    numMeasurements = 0;
                    ctx = MetadataManager.INSTANCE.beginTransaction();
                    MetadataManager.INSTANCE.registerFeedActivity(ctx, feedId, feedActivity);
                    MetadataManager.INSTANCE.commitTransaction(ctx);
                    count.delete(0, count.length() - 1);
                    rate.delete(0, rate.length() - 1);
                }
                previousCount = currentCount;
            } catch (Exception e) {
                if (ctx != null) {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                }
            }
        }
    }

}
